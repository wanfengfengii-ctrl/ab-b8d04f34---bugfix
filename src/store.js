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
import { hostname } from 'node:os';
import path from 'node:path';

const HEX64 = /^[a-f0-9]{64}$/i;
export const VERSION_RE = /^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$/;

// 跨实例活动切换锁（共享 DATA_DIR 的多实例部署：滚动重启/主备/共享持久卷）。
// 按型号一个锁文件，open('wx') 原子创建即获得；持有者崩溃残留的锁超过
// STALE 毫秒未更新即被等待者经原子 rename 接管。临界区仅含小文件读写，
// 亚秒级即可完成，TTL 余量充足；WAIT 略大于 STALE，保证必能在超时前接管死锁。
const ACTIVE_LOCK_STALE_MS = 10_000;
const ACTIVE_LOCK_WAIT_MS = 15_000;

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
    /** @type {Map<string, Promise<unknown>>} 按型号串行化活动切换，杜绝并发竞争产生混合状态 */
    this.activeLocks = new Map();
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
      try {
        const meta = JSON.parse(await readFile(path.join(this.releasesDir, e.name, 'meta.json'), 'utf8'));
        if (!meta || meta.version !== e.name || !HEX64.test(meta.sha256 || '')) continue;
        await access(path.join(this.releasesDir, e.name, 'artifact'));
        this.meta.set(meta.version, Object.freeze({ ...meta }));
      } catch { /* 残缺目录：忽略，不污染已发布视图 */ }
    }

    // 载入型号 -> 活动版本映射。映射只指向不可变发布件：
    // 指向缺失/损坏发布件的记录视为孤儿，忽略（绝不回退到其他版本拼接）。
    let actEntries = [];
    try {
      actEntries = await readdir(this.activeDir, { withFileTypes: true });
    } catch { /* ignore */ }
    // 清理崩溃残留的原子写临时文件与锁接管临时文件
    await Promise.all(
      actEntries
        .filter((e) => e.isFile() && (e.name.includes('.tmp-') || e.name.includes('.lock.steal-')))
        .map((e) => rm(path.join(this.activeDir, e.name), { force: true }).catch(() => {})),
    );
    for (const e of actEntries) {
      if (!e.isFile() || !e.name.endsWith('.json')) continue;
      try {
        const rec = parseActiveRecord(await readFile(path.join(this.activeDir, e.name), 'utf8'));
        if (!rec || e.name !== encodeModel(rec.targetModel)) continue;
        const meta = this.meta.get(rec.version);
        if (!meta || meta.targetModel !== rec.targetModel) continue;
        this.active.set(rec.targetModel, Object.freeze(rec));
      } catch { /* 残缺映射文件：忽略 */ }
    }
  }

  list() {
    return [...this.meta.values()].sort((a, b) => a.publishedAt.localeCompare(b.publishedAt));
  }

  get(version) {
    return this.meta.get(version) || null;
  }

  /**
   * 发布件元数据：内存优先，未命中时从共享盘惰性加载并缓存。
   * 多实例共享 DATA_DIR 时，发布可能由其他实例完成，磁盘是唯一权威状态。
   */
  async getFresh(version) {
    const hit = this.meta.get(version);
    if (hit) return hit;
    if (!VERSION_RE.test(version)) return null;
    try {
      const meta = JSON.parse(
        await readFile(path.join(this.releasesDir, version, 'meta.json'), 'utf8'),
      );
      if (!meta || meta.version !== version || !HEX64.test(meta.sha256 || '')) return null;
      await access(path.join(this.releasesDir, version, 'artifact'));
      const frozen = Object.freeze({ ...meta });
      this.meta.set(version, frozen);
      return frozen;
    } catch {
      return null;
    }
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

  /** 某型号当前活动发布件的不可变元数据；从未切换返回 null */
  getActiveMeta(targetModel) {
    const rec = this.active.get(targetModel);
    return rec ? this.meta.get(rec.version) || null : null;
  }

  /**
   * 从共享盘刷新某型号的活动映射，返回权威记录（无映射返回 null）。
   * 多实例共享 DATA_DIR 时，磁盘上的映射文件是唯一权威状态（竞争切换的
   * 获胜者由它决定），本实例内存中的映射只是它的缓存：每次读取都对齐磁盘，
   * 因此竞争结束后无论请求落到哪个实例，都解析到同一个获胜版本。
   */
  async refreshActive(targetModel) {
    let raw = null;
    try {
      raw = await readFile(path.join(this.activeDir, encodeModel(targetModel)), 'utf8');
    } catch { /* 文件不存在（或暂不可读）：共享状态视为无映射 */ }
    const rec = raw === null ? null : parseActiveRecord(raw, targetModel);
    if (rec) {
      const frozen = Object.freeze(rec);
      this.active.set(targetModel, frozen);
      return frozen;
    }
    this.active.delete(targetModel);
    return null;
  }

  /**
   * 设备稳定地址读取：先对齐共享盘上的权威活动映射，再解析到该版本
   * 不可变发布件的元数据。调用方在请求开始即锁定单一版本，响应期间
   * 发生的切换不会混入本次响应。
   */
  async getActiveMetaFresh(targetModel) {
    const rec = await this.refreshActive(targetModel);
    return rec ? this.getFresh(rec.version) : null;
  }

  /**
   * 将某型号的活动固件切换为指定发布版本。
   * - 发布件不存在：RELEASE_NOT_FOUND(404)
   * - 发布件型号与目标型号不一致：MODEL_MISMATCH(409)
   * - expectedVersion 与切换前活动版本不符（首次切换须为 null）：VERSION_CONFLICT(409)
   * 临界区为“跨实例文件锁 → 读共享盘上的权威映射 → 校验 → 原子替换文件 → 更新内存”。
   * 共享同一 DATA_DIR 的多个实例经同一锁文件串行进入，expectedVersion 乐观校验
   * 针对磁盘上的最新映射执行，因此同一前置版本上的并发竞争（无论是否跨实例）
   * 至多一个请求成功，其余拿到 409 且已确定的活动版本不被改变。
   */
  async setActive(targetModel, releaseVersion, expectedVersion) {
    if (!targetModel || targetModel.length > 128) {
      throw new StoreError('targetModel 缺失或过长（最长 128）', 400, 'INVALID_TARGET_MODEL');
    }
    return this.#withActiveLock(targetModel, async () => {
      // 跨实例互斥：共享 DATA_DIR 的所有实例经同一锁文件串行进入临界区
      const releaseFileLock = await this.#acquireActiveFileLock(targetModel);
      try {
        const meta = await this.getFresh(releaseVersion);
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
        // 比较并交换：expectedVersion 必须与共享盘上的当前活动版本一致。
        // 另一实例刚完成的切换在这里立即可见，故竞争失败者必然校验失败。
        const current = await this.refreshActive(targetModel);
        const currentVersion = current ? current.version : null;
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
        // 先写新映射文件再更新内存；原子 rename 保证任何实例读到的都是完整记录
        const file = path.join(this.activeDir, encodeModel(targetModel));
        await writeFileAtomic(file, JSON.stringify(record, null, 2) + '\n');
        const frozen = Object.freeze({ ...record });
        this.active.set(targetModel, frozen);
        return frozen;
      } finally {
        await releaseFileLock();
      }
    });
  }

  /**
   * 跨进程活动切换锁：active/ 下按型号一个锁文件，open('wx') 原子创建即获得。
   * 持有者崩溃残留的锁超过 ACTIVE_LOCK_STALE_MS 未更新时，等待者经原子 rename
   * 抢占接管（rename 只有一个胜者，不存在 rm+create 的双占窗口）。
   */
  async #acquireActiveFileLock(targetModel) {
    const lockPath = path.join(this.activeDir, `${encodeModel(targetModel)}.lock`);
    const deadline = Date.now() + ACTIVE_LOCK_WAIT_MS;
    for (;;) {
      try {
        const fh = await open(lockPath, 'wx');
        try {
          await fh.writeFile(JSON.stringify({
            pid: process.pid,
            host: hostname(),
            at: new Date().toISOString(),
          }));
        } finally {
          await fh.close();
        }
        return async () => { await rm(lockPath, { force: true }).catch(() => {}); };
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
      }
      // 锁已被其他实例持有：过期则原子接管，否则短暂退避后重试
      let stale = false;
      try {
        const st = await stat(lockPath);
        stale = Date.now() - st.mtimeMs > ACTIVE_LOCK_STALE_MS;
      } catch {
        continue; // 锁刚被释放，立即重试
      }
      if (stale) {
        const steal = `${lockPath}.steal-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
        try {
          await rename(lockPath, steal);
          await rm(steal, { force: true }).catch(() => {});
        } catch { /* 其他实例已抢先接管 */ }
        continue;
      }
      if (Date.now() > deadline) {
        throw new StoreError(
          `型号 ${targetModel} 的活动切换锁等待超时，请重试`,
          503,
          'ACTIVE_LOCK_TIMEOUT',
        );
      }
      await sleep(15 + Math.random() * 25);
    }
  }

  /** 按型号串行执行活动切换临界区 */
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
}

async function writeFileAtomic(p, data) {
  const tmp = `${p}.tmp-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  await writeFile(tmp, data, { encoding: 'utf8' });
  await rename(tmp, p);
}

/**
 * 解析活动映射文件内容；expectedModel 提供时校验记录型号一致。
 * 内容残缺/非法返回 null（与“无映射”同等处理，绝不回退拼接其他版本）。
 */
function parseActiveRecord(raw, expectedModel) {
  let rec;
  try {
    rec = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!rec || typeof rec.targetModel !== 'string' || typeof rec.version !== 'string') return null;
  if (expectedModel !== undefined && rec.targetModel !== expectedModel) return null;
  return {
    targetModel: rec.targetModel,
    version: rec.version,
    switchedAt: typeof rec.switchedAt === 'string' ? rec.switchedAt : '',
  };
}

function sleep(ms) {
  return new Promise((resolve) => { setTimeout(resolve, ms); });
}

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
