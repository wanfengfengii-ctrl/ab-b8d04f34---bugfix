// 固件发布存储：持久化到 DATA_DIR，发布动作原子化，重启后既有版本仍可下载。
// 目录结构：
//   <data>/staging/<id>/{artifact,meta.json}        发布中的暂存区
//   <data>/releases/<version>/{artifact,meta.json}  已发布（不可变）
//   <data>/active/<encodedModel>.json               型号 -> 活动版本映射（原子写入）
import {
  createReadStream,
  createWriteStream,
} from 'node:fs';
import {
  access,
  mkdir,
  open,
  readdir,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { createHash } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';

const HEX64 = /^[a-f0-9]{64}$/i;
export const VERSION_RE = /^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$/;

// 跨进程活动切换锁参数。临界区只含几次毫秒级文件操作：
// 锁文件存活超过该阈值只能说明持锁进程崩溃/被强杀，等待者可安全接管。
const ACTIVE_LOCK_STALE_MS = 10_000;
const ACTIVE_LOCK_WAIT_MS = 30_000;

export class StoreError extends Error {
  constructor(message, status = 400, code = 'BAD_REQUEST') {
    super(message);
    this.name = 'StoreError';
    this.status = status;
    this.code = code;
  }
}

export class FirmwareStore {
  constructor(dataDir) {
    this.dataDir = dataDir;
    this.releasesDir = path.join(dataDir, 'releases');
    this.stagingDir = path.join(dataDir, 'staging');
    this.activeDir = path.join(dataDir, 'active');
    /** @type {Map<string, any>} version -> meta */
    this.meta = new Map();
    /** @type {Map<string, any>} targetModel -> 活动记录 {version, switchedAt} */
    this.active = new Map();
    /** @type {Map<string, Promise<unknown>>} 按型号串行化活动切换（进程内公平排队） */
    this.activeLocks = new Map();
    /** 跨进程互斥锁令牌：{name, token}，未持锁为 null */
    this.crossProcLock = null;
    // 锁文件中标识持锁进程/实例，仅用于排障与存活时间判断
    this.instanceId = `${os.hostname()}-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  }

  async init() {
    await mkdir(this.releasesDir, { recursive: true });
    await mkdir(this.stagingDir, { recursive: true });
    await mkdir(this.activeDir, { recursive: true });
    // 清理上次崩溃残留的暂存区
    let entries = [];
    try {
      entries = await readdir(this.stagingDir, { withFileTypes: true });
    } catch { /* ignore */ }
    await Promise.all(
      entries
        .filter((e) => e.isDirectory())
        .map((e) => rm(path.join(this.stagingDir, e.name), { recursive: true, force: true })),
    );

    // 载入已发布版本（缺少 meta.json 的残缺目录直接忽略）
    const rels = await readdir(this.releasesDir, { withFileTypes: true });
    for (const e of rels) {
      if (!e.isDirectory()) continue;
      await this.#loadRelease(e.name);
    }

    // 载入型号 -> 活动版本映射。映射只指向不可变发布件：
    // 指向缺失/损坏发布件的记录视为孤儿，忽略（绝不回退到其他版本拼接）。
    let actEntries = [];
    try {
      actEntries = await readdir(this.activeDir, { withFileTypes: true });
    } catch { /* ignore */ }
    // 清理崩溃残留的原子写临时文件，以及上轮进程崩溃留下的陈旧切换锁
    const now = Date.now();
    await Promise.all(
      actEntries
        .filter((e) => e.isFile() && e.name.includes('.tmp-'))
        .map((e) => rm(path.join(this.activeDir, e.name), { force: true }).catch(() => {})),
    );
    const staleLock = actEntries.find((e) => e.isFile() && e.name === '.switch.lock');
    if (staleLock) {
      try {
        const st = await stat(path.join(this.activeDir, '.switch.lock'));
        if (now - st.mtimeMs > ACTIVE_LOCK_STALE_MS) {
          await rm(path.join(this.activeDir, '.switch.lock'), { force: true });
        }
      } catch { /* 锁已消失：忽略 */ }
    }
    for (const e of actEntries) {
      if (!e.isFile() || !e.name.endsWith('.json')) continue;
      const rec = await this.#readActiveFile(path.join(this.activeDir, e.name));
      if (rec) this.active.set(rec.targetModel, Object.freeze({ ...rec }));
    }
  }

  /**
   * 从磁盘载入一个已发布版本到内存（发布件不可变，可安全缓存/懒加载）。
   * 残缺/校验不过的目录返回 null 且不污染视图。
   */
  async #loadRelease(version) {
    if (this.meta.has(version)) return this.meta.get(version);
    try {
      const dir = path.join(this.releasesDir, version);
      const meta = JSON.parse(await readFile(path.join(dir, 'meta.json'), 'utf8'));
      if (!meta || meta.version !== version || !HEX64.test(meta.sha256 || '')) return null;
      await access(path.join(dir, 'artifact'));
      const frozen = Object.freeze({ ...meta });
      this.meta.set(version, frozen);
      return frozen;
    } catch { /* 残缺目录：忽略 */ return null; }
  }

  /**
   * 读取并校验一个活动映射文件：结构、文件名与型号的对应、
   * 目标发布件存在且型号一致（缺失时从磁盘懒加载发布元数据）。
   * 任何不符都返回 null（孤儿/损坏记录绝不参与拼接响应）。
   */
  async #readActiveFile(file) {
    let raw;
    try {
      raw = await readFile(file, 'utf8');
    } catch { return null; }
    let rec;
    try {
      rec = JSON.parse(raw);
    } catch { return null; }
    if (!rec || typeof rec.targetModel !== 'string' || typeof rec.version !== 'string') return null;
    if (path.basename(file) !== encodeModel(rec.targetModel)) return null;
    const meta = this.meta.get(rec.version) || await this.#loadRelease(rec.version);
    if (!meta || meta.targetModel !== rec.targetModel) return null;
    return {
      targetModel: rec.targetModel,
      version: rec.version,
      switchedAt: typeof rec.switchedAt === 'string' ? rec.switchedAt : '',
    };
  }

  list() {
    return [...this.meta.values()].sort((a, b) => a.publishedAt.localeCompare(b.publishedAt));
  }

  get(version) {
    return this.meta.get(version) || null;
  }

  /** 按版本取发布件元数据；内存缺失时从共享磁盘懒加载（其他实例可能刚发布） */
  async findRelease(version) {
    return this.meta.get(version) || this.#loadRelease(version);
  }

  artifactPath(version) {
    return path.join(this.releasesDir, version, 'artifact');
  }

  /**
   * 原子发布一个已上传的临时文件。
   * 校验失败（缺字段/摘要不符/版本重复）时绝不写入 releases 目录。
   */
  async publish({ fields, file }) {
    const version = (fields.version || '').trim();
    const targetModel = (fields.targetModel || '').trim();
    const claimedSha = (fields.sha256 || '').trim().toLowerCase();

    if (!VERSION_RE.test(version)) {
      throw new StoreError('version 缺失或非法（允许 1-64 位字母数字与 ._+-，且首字符为字母数字）', 400, 'INVALID_VERSION');
    }
    if (!targetModel || targetModel.length > 128) {
      throw new StoreError('targetModel 缺失或过长（最长 128）', 400, 'INVALID_TARGET_MODEL');
    }
    if (!HEX64.test(claimedSha)) {
      throw new StoreError('sha256 缺失或不是 64 位十六进制摘要', 400, 'INVALID_SHA256');
    }
    if (!file) {
      throw new StoreError('缺少 artifact 文件字段', 400, 'MISSING_ARTIFACT');
    }

    // 1) 摘要校验（解析器已流式计算过一遍）。不符则丢弃临时文件，不得落盘到发布区。
    if (file.sha256.toLowerCase() !== claimedSha) {
      await rm(file.path, { force: true }).catch(() => {});
      throw new StoreError(
        `摘要校验失败：期望 ${claimedSha}，实际 ${file.sha256.toLowerCase()}`,
        422,
        'SHA256_MISMATCH',
      );
    }

    if (this.meta.has(version)) {
      await rm(file.path, { force: true }).catch(() => {});
      throw new StoreError(`版本 ${version} 已发布，已发布版本不可覆盖`, 409, 'VERSION_EXISTS');
    }

    // 2) 暂存目录中组装 artifact + meta.json，再整体原子改名到 releases/<version>
    const id = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
    const stage = path.join(this.stagingDir, id);
    await mkdir(stage, { recursive: true });
    try {
      const stagedArtifact = path.join(stage, 'artifact');
      await rename(file.path, stagedArtifact);

      // 防御性二次校验：从暂存文件重新流式计算摘要，确保落盘字节与声明一致
      const actualSha = await hashFile(stagedArtifact);
      if (actualSha !== claimedSha) {
        throw new StoreError('暂存文件摘要与声明不符', 422, 'SHA256_MISMATCH');
      }
      const st = await stat(stagedArtifact);

      const meta = {
        version,
        targetModel,
        sha256: claimedSha,
        size: st.size,
        filename: file.filename,
        contentType: file.contentType,
        publishedAt: new Date().toISOString(),
      };
      await writeFileAtomic(path.join(stage, 'meta.json'), JSON.stringify(meta, null, 2) + '\n');

      const finalDir = path.join(this.releasesDir, version);
      try {
        // POSIX 上 rename 到已存在的非空目录会失败（ENOTEMPTY/EEXIST），
        // 由此挡住并发的重复版本发布，且不会破坏既有发布。
        await rename(stage, finalDir);
      } catch (err) {
        if (err.code === 'ENOTEMPTY' || err.code === 'EEXIST') {
          // 并发抢发：重新确认既有版本并报 409
          if (this.meta.has(version) || await pathExists(path.join(finalDir, 'meta.json'))) {
            throw new StoreError(`版本 ${version} 已发布，已发布版本不可覆盖`, 409, 'VERSION_EXISTS');
          }
        }
        throw err;
      }

      const frozen = Object.freeze({ ...meta });
      this.meta.set(version, frozen);
      return frozen;
    } finally {
      await rm(stage, { recursive: true, force: true }).catch(() => {});
      // 若临时文件仍在（rename 未执行），一并清理
      await rm(file.path, { force: true }).catch(() => {});
    }
  }

  /** 打开某版本 artifact 的只读流（供范围/全量响应使用） */
  openArtifact(version, range) {
    const p = this.artifactPath(version);
    if (range) return createReadStream(p, { start: range.start, end: range.end });
    return createReadStream(p);
  }

  /** 某型号当前活动记录 {targetModel, version, switchedAt}，未切换过返回 null */
  getActive(targetModel) {
    return this.active.get(targetModel) || null;
  }

  /**
   * 从共享磁盘重读某型号的活动映射并刷新本实例内存。
   * 多实例共用 DATA_DIR 时，竞争的赢家可能在另一实例上落盘：
   * 设备每次稳定取件前都以磁盘为准，确保所有实例立即收敛到唯一获胜版本，
   * 不再继续提供本实例先前看到的旧活动版本。返回磁盘上的活动记录（无则 null）。
   */
  async refreshActiveFromDisk(targetModel) {
    const file = path.join(this.activeDir, encodeModel(targetModel));
    const rec = await this.#readActiveFile(file);
    if (rec) {
      this.active.set(targetModel, Object.freeze({ ...rec }));
    } else {
      // 映射文件不存在/损坏/孤儿：磁盘上没有有效活动版本，同步清空缓存
      this.active.delete(targetModel);
    }
    return rec;
  }

  /** 某型号当前活动发布件的不可变元数据；从未切换返回 null */
  getActiveMeta(targetModel) {
    const rec = this.active.get(targetModel);
    return rec ? this.meta.get(rec.version) || null : null;
  }

  /**
   * 将某型号的活动固件切换为指定发布版本。
   * - 发布件不存在：RELEASE_NOT_FOUND(404)
   * - 发布件型号与目标型号不一致：MODEL_MISMATCH(409)
   * - expectedVersion 与切换前活动版本不符（首次切换须为 null）：VERSION_CONFLICT(409)
   * 临界区为“跨进程文件锁 → 以共享磁盘为准重读旧映射 → 校验 → 原子替换文件 →
   * 更新内存”：同一 DATA_DIR 上的多个实例也只有一个能在旧版本上 CAS 成功，
   * 竞争失败者拿到 409，活动版本保持为赢家版本，进程内再按型号串行排队。
   */
  async setActive(targetModel, releaseVersion, expectedVersion) {
    if (!targetModel || targetModel.length > 128) {
      throw new StoreError('targetModel 缺失或过长（最长 128）', 400, 'INVALID_TARGET_MODEL');
    }
    return this.#withActiveLock(targetModel, async () => {
      await this.#acquireActiveLock();
      try {
        // 持锁期间没有任何实例能写活动目录；以共享磁盘上的映射为 CAS 旧值
        const current = await this.refreshActiveFromDisk(targetModel);
        const currentVersion = current ? current.version : null;

        // 发布件不可变：内存缺失时从磁盘懒加载（其他实例可能刚发布）
        const meta = this.meta.get(releaseVersion) || await this.#loadRelease(releaseVersion);
        if (!meta) {
          throw new StoreError(`发布版本 ${releaseVersion} 不存在`, 404, 'RELEASE_NOT_FOUND');
        }
        if (meta.targetModel !== targetModel) {
          throw new StoreError(
            `发布版本 ${releaseVersion} 的型号为 ${meta.targetModel}，无法切换到型号 ${targetModel}`,
            409,
            'MODEL_MISMATCH',
          );
        }
        if (expectedVersion !== currentVersion) {
          throw new StoreError(
            currentVersion === null
              ? `型号 ${targetModel} 从未切换活动固件，expectedVersion 必须为 null`
              : `前置版本不匹配：expectedVersion=${formatExpected(expectedVersion)}，当前活动版本为 ${currentVersion}`,
            409,
            'VERSION_CONFLICT',
          );
        }

        const record = {
          targetModel,
          version: releaseVersion,
          switchedAt: new Date().toISOString(),
        };
        // 先写新映射文件再更新内存；原子 rename 保证任何实例/重启看到的都是完整记录
        const file = path.join(this.activeDir, encodeModel(targetModel));
        await writeFileAtomic(file, JSON.stringify(record, null, 2) + '\n');
        const frozen = Object.freeze({ ...record });
        this.active.set(targetModel, frozen);
        return frozen;
      } finally {
        await this.#releaseActiveLock();
      }
    });
  }

  /** 按型号串行执行活动切换临界区（进程内公平排队，跨进程互斥见文件锁） */
  async #withActiveLock(targetModel, fn) {
    const prev = this.activeLocks.get(targetModel) || Promise.resolve();
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    // 后继者无论前任成败都要进入，但必须等前任完全退出临界区
    this.activeLocks.set(targetModel, prev.then(() => gate, () => gate));
    try {
      await prev;
      return await fn();
    } finally {
      release();
    }
  }

  /**
   * 获取跨进程活动切换锁：在共享 active/ 目录中以 O_EXCL 创建锁文件。
   * 同一进程的切换已由 #withActiveLock 串行，故此处至多一次未决获取；
   * 等待者轮询，锁文件超过 ACTIVE_LOCK_STALE_MS 视为持锁进程崩溃并强制接管。
   */
  async #acquireActiveLock() {
    const lockName = '.switch.lock';
    const lockPath = path.join(this.activeDir, lockName);
    const deadline = Date.now() + ACTIVE_LOCK_WAIT_MS;
    let warnedStale = false;
    for (;;) {
      const token = `${this.instanceId}:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`;
      let fh;
      try {
        fh = await open(lockPath, 'wx');
        try {
          await fh.writeFile(token, 'utf8');
        } catch (writeErr) {
          // 建锁成功但写令牌失败：删除空锁后上抛，避免留下立即被判陈旧的空锁
          await rm(lockPath, { force: true }).catch(() => {});
          throw writeErr;
        }
        this.crossProcLock = { name: lockName, token };
        return;
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
      } finally {
        if (fh) await fh.close().catch(() => {});
      }

      // 锁被占用：检查是否为陈旧锁（持锁进程已死 / 临界区异常耗时）
      let ageMs = null;
      try {
        const st = await stat(lockPath);
        ageMs = Date.now() - st.mtimeMs;
      } catch (err) {
        if (err.code === 'ENOENT') continue; // 持锁者刚释放，立刻重试
        throw err;
      }
      if (ageMs !== null && ageMs > ACTIVE_LOCK_STALE_MS) {
        // rm 后由下一轮 O_EXCL 仲裁：无论多少等待者同时判定陈旧，都只有一个能建锁
        if (!warnedStale) {
          warnedStale = true;
          console.warn(`[firmware] 活动切换锁已陈旧（${Math.round(ageMs)}ms），强制接管：${lockPath}`);
          await rm(lockPath, { force: true }).catch(() => {});
        }
      } else {
        // 观察到新鲜锁后复位：若新持锁者也崩溃，下一轮仍可正常接管
        warnedStale = false;
      }
      if (Date.now() > deadline) {
        throw new StoreError(
          '活动固件切换锁在 30s 内不可用（共享存储可能不可达）',
          503,
          'ACTIVE_LOCK_BUSY',
        );
      }
      await sleep(20 + Math.random() * 30);
    }
  }

  /** 释放跨进程锁：只删除自己持有的锁（令牌不符则绝不动别人的锁） */
  async #releaseActiveLock() {
    const held = this.crossProcLock;
    this.crossProcLock = null;
    if (!held) return;
    const lockPath = path.join(this.activeDir, held.name);
    try {
      const cur = await readFile(lockPath, 'utf8');
      if (cur === held.token) await rm(lockPath, { force: true });
    } catch { /* 锁文件缺失或已被陈旧接管：无需处理 */ }
  }
}

async function writeFileAtomic(p, data) {
  const tmp = `${p}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  await writeFile(tmp, data, { encoding: 'utf8' });
  await rename(tmp, p);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function pathExists(p) {
  try {
    await access(p);
    return true;
  } catch {
    return false;
  }
}

function hashFile(p) {
  return new Promise((resolve, reject) => {
    const hash = createHash('sha256');
    const rs = createReadStream(p);
    rs.on('error', reject);
    rs.on('data', (c) => hash.update(c));
    rs.on('end', () => resolve(hash.digest('hex')));
  });
}

/**
 * 型号名 -> 活动映射文件名。型号可含非 ASCII 字符与路径分隔符，不能直接做文件名，
 * 也不宜做转义（转义结果仍可能超出 255 字节限制），故采用 SHA-256 定长摘要；
 * 映射文件内部完整记录 targetModel，载入时重新核对。
 */
function encodeModel(model) {
  return `model-${createHash('sha256').update(model, 'utf8').digest('hex')}.json`;
}

function formatExpected(v) {
  return v === null ? 'null' : JSON.stringify(v);
}

// 供上传解析器使用的 createWriteStream 再导出（保持引用集中）
export { createWriteStream };
