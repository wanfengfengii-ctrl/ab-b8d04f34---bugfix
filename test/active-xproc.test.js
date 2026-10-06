// 跨实例（跨 OS 进程）活动版本切换回归：
// 两个（及三个）服务实例使用同一 DATA_DIR，模拟滚动重启 / 主备切换 /
// 共享持久卷部署。验证：
//   - 同一旧版本上的竞争切换遵循统一 CAS：至多一个 200，其余 409 VERSION_CONFLICT
//   - 竞争结束后任一实例的稳定地址（GET/HEAD/Range/If-Range/416）都解析到唯一获胜版本
//   - 落败实例不会继续提供自己先前看到的旧活动版本（本测试所针对的分裂缺陷）
//   - 无竞争首切/后续正常切换、发布件不可变、型号匹配与 expectedVersion 既有规则
//   - 实例重启 / 新实例加入后仍给出唯一一致的活动固件
//   - 持锁进程崩溃留下的陈旧锁可被接管
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import net from 'node:net';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { FirmwareStore } from '../src/store.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const serverPath = path.join(here, '..', 'src', 'server.js');
const sha = (buf) => createHash('sha256').update(buf).digest('hex');

let root;
let instances = [];
const bases = {}; // A/B/C -> http://127.0.0.1:port

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

async function startInstance(name) {
  const port = await freePort();
  const child = spawn(process.execPath, [serverPath], {
    env: {
      ...process.env,
      DATA_DIR: root,
      PORT: String(port),
      HOST: '127.0.0.1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  const base = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 20_000;
  for (;;) {
    try {
      const r = await fetch(`${base}/healthz`);
      if (r.ok) break;
    } catch { /* 尚未监听 */ }
    if (Date.now() > deadline) {
      child.kill('SIGKILL');
      throw new Error(`实例 ${name} 未在 20s 内通过健康检查`);
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  instances.push({ name, child, base });
  bases[name] = base;
  return base;
}

async function stopInstance(name) {
  const inst = instances.find((x) => x.name === name);
  if (!inst) return;
  inst.child.kill('SIGTERM');
  await once(inst.child, 'exit').catch(() => {});
  instances = instances.filter((x) => x.name !== name);
  delete bases[name];
}

before(async () => {
  root = await mkdtemp(path.join(tmpdir(), 'fw-xproc-'));
  // 两个实例在任何数据存在之前就已启动：它们必须靠共享磁盘在运行期收敛，
  // 而不是仅靠启动时加载。
  await startInstance('A');
  await startInstance('B');
});

after(async () => {
  for (const { child } of instances) child.kill('SIGKILL');
  await rm(root, { recursive: true, force: true });
});

async function publish(base, version, targetModel, data) {
  const fd = new FormData();
  fd.set('version', version);
  fd.set('targetModel', targetModel);
  fd.set('sha256', sha(data));
  fd.set('artifact', new Blob([data], { type: 'application/octet-stream' }), `${version}.bin`);
  const r = await fetch(`${base}/api/firmware/releases`, { method: 'POST', body: fd });
  assert.equal(r.status, 201, `发布 ${version} 应成功（status=${r.status}）`);
  return r;
}

const switchUrl = (base, m) => `${base}/api/firmware/models/${encodeURIComponent(m)}/active`;
const stableUrl = (base, m) => `${base}/api/firmware/models/${encodeURIComponent(m)}/artifact`;

function switchActive(base, model, body) {
  return fetch(switchUrl(base, model), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

async function fullGet(base, model) {
  const r = await fetch(stableUrl(base, model));
  return { r, buf: Buffer.from(await r.arrayBuffer()) };
}

const MODEL = 'WT-MULTI';
const V0 = Buffer.from('WT-MULTI-v0-'.repeat(900));
const V1 = Buffer.from('WT-MULTI-v1-'.repeat(900));
const V2 = Buffer.from('WT-MULTI-v2-'.repeat(900));
let winner;

test('准备：经实例 A 发布 v0/v1/v2', async () => {
  await publish(bases.A, 'v0', MODEL, V0);
  await publish(bases.B, 'v1', MODEL, V1); // 另一实例发布，A 须能懒加载
  await publish(bases.A, 'v2', MODEL, V2);
  // 发布件不可变：重复发布 409
  const dupFd = new FormData();
  dupFd.set('version', 'v0');
  dupFd.set('targetModel', MODEL);
  dupFd.set('sha256', sha(Buffer.from('x')));
  dupFd.set('artifact', new Blob([Buffer.from('x')]), 'v0.bin');
  const dup = await fetch(`${bases.B}/api/firmware/releases`, { method: 'POST', body: dupFd });
  assert.equal(dup.status, 409);
  assert.equal((await dup.json()).error.code, 'VERSION_EXISTS');
});

test('无竞争首次切换（expectedVersion=null）在实例 A 成功', async () => {
  const bad = await switchActive(bases.A, MODEL, { releaseVersion: 'v0', expectedVersion: 'v9' });
  assert.equal(bad.status, 409);
  assert.equal((await bad.json()).error.code, 'VERSION_CONFLICT');

  const ok = await switchActive(bases.A, MODEL, { releaseVersion: 'v0', expectedVersion: null });
  assert.equal(ok.status, 200);
  assert.equal(ok.headers.get('etag'), `"${sha(V0)}"`);
  assert.equal((await ok.json()).activeVersion, 'v0');
});

test('尚未参与过切换的实例 B 从共享磁盘读到同一活动版本 v0', async () => {
  const { r, buf } = await fullGet(bases.B, MODEL);
  assert.equal(r.status, 200);
  assert.equal(r.headers.get('etag'), `"${sha(V0)}"`);
  assert.ok(buf.equals(V0));
});

test('双实例并发竞争（expectedVersion 均为 v0，分别切 v1/v2）：恰一个 200、一个 409', async () => {
  const [r1, r2] = await Promise.all([
    switchActive(bases.A, MODEL, { releaseVersion: 'v1', expectedVersion: 'v0' }),
    switchActive(bases.B, MODEL, { releaseVersion: 'v2', expectedVersion: 'v0' }),
  ]);
  const statuses = [r1.status, r2.status].sort((a, b) => a - b);
  assert.deepEqual(statuses, [200, 409], `竞争结果应为 200+409，实际 ${statuses}`);
  const loser = [r1, r2].find((r) => r.status === 409);
  assert.equal((await loser.json()).error.code, 'VERSION_CONFLICT');
  const win = [r1, r2].find((r) => r.status === 200);
  winner = (await win.json()).activeVersion;
  assert.ok(winner === 'v1' || winner === 'v2');
});

test('竞争后跨实例读取：两个实例的稳定地址都只提供获胜版本', async () => {
  const winBuf = winner === 'v1' ? V1 : V2;
  for (const name of ['A', 'B']) {
    const { r, buf } = await fullGet(bases[name], MODEL);
    assert.equal(r.status, 200, `实例 ${name}`);
    assert.equal(r.headers.get('etag'), `"${sha(winBuf)}"`, `实例 ${name} ETag 分裂`);
    assert.ok(buf.equals(winBuf), `实例 ${name} 仍提供落败版本字节`);

    const head = await fetch(stableUrl(bases[name], MODEL), { method: 'HEAD' });
    assert.equal(head.status, 200);
    assert.equal(head.headers.get('etag'), `"${sha(winBuf)}"`);
    assert.equal(Number(head.headers.get('content-length')), winBuf.length);
    assert.equal(await head.text(), '');
  }
});

test('竞争后两实例的 Range/206/416/If-Range 语义保持且响应不混合版本', async () => {
  const winBuf = winner === 'v1' ? V1 : V2;
  for (const name of ['A', 'B']) {
    const url = stableUrl(bases[name], MODEL);

    const part = await fetch(url, { headers: { Range: 'bytes=50-199' } });
    assert.equal(part.status, 206, `实例 ${name}`);
    assert.equal(part.headers.get('etag'), `"${sha(winBuf)}"`);
    assert.equal(part.headers.get('content-range'), `bytes 50-199/${winBuf.length}`);
    assert.ok(Buffer.from(await part.arrayBuffer()).equals(winBuf.subarray(50, 200)));

    const unsat = await fetch(url, { headers: { Range: `bytes=${winBuf.length}-` } });
    assert.equal(unsat.status, 416);
    assert.equal(unsat.headers.get('content-range'), `bytes */${winBuf.length}`);
    assert.equal(unsat.headers.get('etag'), `"${sha(winBuf)}"`);
    await unsat.arrayBuffer().catch(() => {});

    // If-Range 为获胜版本 ETag → 206；为落败版本 ETag → 回退完整 200，且字节是获胜版本
    const matched = await fetch(url, {
      headers: { Range: 'bytes=0-49', 'If-Range': `"${sha(winBuf)}"` },
    });
    assert.equal(matched.status, 206, `实例 ${name}`);

    const loseBuf = winner === 'v1' ? V2 : V1;
    const stale = await fetch(url, {
      headers: { Range: 'bytes=0-49', 'If-Range': `"${sha(loseBuf)}"` },
    });
    assert.equal(stale.status, 200);
    assert.equal(Number(stale.headers.get('content-length')), winBuf.length);
    assert.ok(Buffer.from(await stale.arrayBuffer()).equals(winBuf));
  }
});

test('落败版本不得成为活动版本；旧 expectedVersion 再切仍 409', async () => {
  const loserVer = winner === 'v1' ? 'v2' : 'v1';
  const retry = await switchActive(bases.B, MODEL, { releaseVersion: loserVer, expectedVersion: 'v0' });
  assert.equal(retry.status, 409);
  assert.equal((await retry.json()).error.code, 'VERSION_CONFLICT');
});

test('后续正常切换：以获胜版本为 expectedVersion 切到另一版本，跨实例立即可见', async () => {
  const next = winner === 'v1' ? 'v2' : 'v1';
  const nextBuf = next === 'v1' ? V1 : V2;
  const ok = await switchActive(bases.B, MODEL, { releaseVersion: next, expectedVersion: winner });
  assert.equal(ok.status, 200);
  assert.equal((await ok.json()).activeVersion, next);
  winner = next;
  for (const name of ['A', 'B']) {
    const { r, buf } = await fullGet(bases[name], MODEL);
    assert.equal(r.headers.get('etag'), `"${sha(nextBuf)}"`, `实例 ${name} 未收敛到 ${next}`);
    assert.ok(buf.equals(nextBuf));
  }
});

test('型号匹配与未知发布规则在跨实例下保持', async () => {
  const other = Buffer.from('other-model-'.repeat(50));
  await publish(bases.A, 'vo', 'WT-OTHER', other);

  const mismatch = await switchActive(bases.A, MODEL, { releaseVersion: 'vo', expectedVersion: winner });
  assert.equal(mismatch.status, 409);
  assert.equal((await mismatch.json()).error.code, 'MODEL_MISMATCH');

  const notFound = await switchActive(bases.B, MODEL, { releaseVersion: 'nope', expectedVersion: winner });
  assert.equal(notFound.status, 404);
  assert.equal((await notFound.json()).error.code, 'RELEASE_NOT_FOUND');

  // 失败请求未改变活动版本
  const { r } = await fullGet(bases.B, MODEL);
  const winBuf = winner === 'v1' ? V1 : V2;
  assert.equal(r.headers.get('etag'), `"${sha(winBuf)}"`);
});

test('N=4 路并发（跨两实例、同一前置版本）仍恰一个成功，全实例收敛', async () => {
  const m = 'WT-RACE';
  const bufs = ['r0', 'r1', 'r2', 'r3', 'r4'].map((t) => Buffer.from(`${t}-`.repeat(200)));
  await publish(bases.A, 'r0', m, bufs[0]);
  for (const v of ['r1', 'r2', 'r3', 'r4']) {
    await publish(bases.A, v, m, bufs[['r1', 'r2', 'r3', 'r4'].indexOf(v) + 1]);
  }
  const first = await switchActive(bases.A, m, { releaseVersion: 'r0', expectedVersion: null });
  assert.equal(first.status, 200);

  const results = await Promise.all([
    switchActive(bases.A, m, { releaseVersion: 'r1', expectedVersion: 'r0' }),
    switchActive(bases.B, m, { releaseVersion: 'r2', expectedVersion: 'r0' }),
    switchActive(bases.A, m, { releaseVersion: 'r3', expectedVersion: 'r0' }),
    switchActive(bases.B, m, { releaseVersion: 'r4', expectedVersion: 'r0' }),
  ]);
  const oks = results.filter((r) => r.status === 200);
  const conflicts = results.filter((r) => r.status === 409);
  assert.equal(oks.length, 1, `4 路竞争应只有 1 个 200，实际 ${oks.length}`);
  assert.equal(conflicts.length, 3);
  const raceWinner = (await oks[0].json()).activeVersion;
  const winBuf = bufs[['r1', 'r2', 'r3', 'r4'].indexOf(raceWinner) + 1];
  for (const name of ['A', 'B']) {
    const { r, buf } = await fullGet(bases[name], m);
    assert.equal(r.headers.get('etag'), `"${sha(winBuf)}"`, `实例 ${name} 竞争后发散`);
    assert.ok(buf.equals(winBuf));
  }
});

test('实例重启与新实例加入：稳定地址仍给出唯一获胜版本', async () => {
  await stopInstance('A');
  await startInstance('C');
  const winBuf = winner === 'v1' ? V1 : V2;
  for (const name of ['B', 'C']) {
    const { r, buf } = await fullGet(bases[name], MODEL);
    assert.equal(r.status, 200);
    assert.equal(r.headers.get('etag'), `"${sha(winBuf)}"`, `实例 ${name} 重启/加入后发散`);
    assert.ok(buf.equals(winBuf));
    // 分段语义同样锁定在获胜版本
    const part = await fetch(stableUrl(bases[name], MODEL), { headers: { Range: 'bytes=0-99' } });
    assert.equal(part.status, 206);
    assert.equal(part.headers.get('etag'), `"${sha(winBuf)}"`);
    assert.ok(Buffer.from(await part.arrayBuffer()).equals(winBuf.subarray(0, 100)));
  }
});

test('持锁进程崩溃留下的陈旧锁会被接管，切换不被永久阻塞', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'fw-stale-lock-'));
  try {
    const store = new FirmwareStore(dir);
    await store.init();
    const data = Buffer.from('stale-lock-recovery');
    const tmp = path.join(dir, `${Math.random().toString(36).slice(2)}.tmp`);
    await writeFile(tmp, data);
    await store.publish({
      fields: { version: '1.0.0', targetModel: 'WT-L', sha256: sha(data) },
      file: {
        path: tmp, size: data.length, sha256: sha(data),
        filename: 'a.bin', contentType: 'application/octet-stream',
      },
    });
    // 伪装成 30s 前崩溃进程遗留的锁
    const lock = path.join(dir, 'active', '.switch.lock');
    await writeFile(lock, 'dead-instance:0:dead');
    const old = new Date(Date.now() - 30_000);
    await utimes(lock, old, old);

    const rec = await store.setActive('WT-L', '1.0.0', null);
    assert.equal(rec.version, '1.0.0');
    assert.equal(store.getActive('WT-L').version, '1.0.0');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
