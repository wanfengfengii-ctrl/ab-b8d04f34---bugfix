// 跨实例一致性回归：两个服务实例共享同一 DATA_DIR（模拟滚动重启 / 主备切换 /
// 共享持久卷部署下多实例同时运行）。验证：
//   1) 共享同一活动映射的竞争切换遵守统一的比较并交换语义——同一前置版本上的
//      两个并发请求至多一个 200，败者 409 VERSION_CONFLICT 且不改变活动版本；
//   2) 竞争结束后，无论请求落到哪个实例，稳定地址都解析到同一个获胜版本，
//      响应正文与 ETag 同属该版本（含 HEAD/Range/If-Range）；
//   3) 无竞争的首次切换与后续正常切换跨实例立即可见；
//   4) 发布件不可变、型号匹配、expectedVersion 既有规则在跨实例下保持；
//   5) 实例重启（新实例加载同一 DATA_DIR）与交替访问后仍给出唯一一致的活动固件。
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { createServer } from '../src/server.js';

let root;
let srvA;
let srvB;
let baseA;
let baseB;

before(async () => {
  root = await mkdtemp(path.join(tmpdir(), 'fw-cluster-'));
  srvA = await startInstance();
  srvB = await startInstance();
  baseA = `http://127.0.0.1:${srvA.address().port}`;
  baseB = `http://127.0.0.1:${srvB.address().port}`;
});

after(async () => {
  await Promise.all([stopInstance(srvA), stopInstance(srvB)]);
  await rm(root, { recursive: true, force: true });
});

async function startInstance() {
  const srv = createServer({ dataDir: root, maxUploadBytes: 8 * 1024 * 1024 });
  await srv.waitForReady();
  await new Promise((resolve) => srv.listen(0, '127.0.0.1', resolve));
  return srv;
}

const stopInstance = (srv) => new Promise((resolve) => srv.close(resolve));

const sha = (buf) => createHash('sha256').update(buf).digest('hex');
const MODEL = 'WT-MULTI';

const V0 = Buffer.from('wt-multi-v0-'.repeat(400));
const V1 = Buffer.from('wt-multi-v1-'.repeat(400));
const V2 = Buffer.from('wt-multi-v2-'.repeat(400));
const BYTES = { v0: V0, v1: V1, v2: V2 };

// 竞争用例的获胜版本与当前活动版本（node:test 顶层用例按序执行）
let raceWinner = null;
let currentVersion = null;

async function publish(base, version, targetModel, data) {
  const fd = new FormData();
  fd.set('version', version);
  fd.set('targetModel', targetModel);
  fd.set('sha256', sha(data));
  fd.set('artifact', new Blob([data], { type: 'application/octet-stream' }), `${version}.bin`);
  return fetch(`${base}/api/firmware/releases`, { method: 'POST', body: fd });
}

const switchUrl = (base, m) => `${base}/api/firmware/models/${encodeURIComponent(m)}/active`;
const stableUrl = (base, m) => `${base}/api/firmware/models/${encodeURIComponent(m)}/artifact`;

const switchActive = (base, model, body) => fetch(switchUrl(base, model), {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

/** 断言某实例稳定地址下发指定版本的完整发布件（GET/HEAD/Range/If-Range 同属一版） */
async function assertServes(base, model, data, label) {
  const url = stableUrl(base, model);
  const etag = `"${sha(data)}"`;

  const full = await fetch(url);
  assert.equal(full.status, 200, `${label}: GET 应 200`);
  assert.equal(full.headers.get('etag'), etag, `${label}: GET ETag 应属于 ${label} 版本`);
  assert.equal(Number(full.headers.get('content-length')), data.length);
  assert.ok(Buffer.from(await full.arrayBuffer()).equals(data), `${label}: GET 字节不一致`);

  const head = await fetch(url, { method: 'HEAD' });
  assert.equal(head.status, 200, `${label}: HEAD 应 200`);
  assert.equal(head.headers.get('etag'), etag, `${label}: HEAD ETag 不一致`);
  assert.equal(Number(head.headers.get('content-length')), data.length);
  assert.equal(await head.text(), '');

  const part = await fetch(url, { headers: { Range: 'bytes=100-199', 'If-Range': etag } });
  assert.equal(part.status, 206, `${label}: If-Range 相符应 206`);
  assert.equal(part.headers.get('etag'), etag);
  assert.equal(part.headers.get('content-range'), `bytes 100-199/${data.length}`);
  assert.ok(Buffer.from(await part.arrayBuffer()).equals(data.subarray(100, 200)));

  const unsat = await fetch(url, { headers: { Range: `bytes=${data.length}-` } });
  assert.equal(unsat.status, 416, `${label}: 越界 Range 应 416`);
  assert.equal(unsat.headers.get('content-range'), `bytes */${data.length}`);
  assert.equal(unsat.headers.get('etag'), etag);
  await unsat.arrayBuffer().catch(() => {});
}

test('准备：v0/v1 经实例 A 发布，v2 经实例 B 发布；跨实例按版本下载可见', async () => {
  for (const [base, v, buf] of [[baseA, 'v0', V0], [baseA, 'v1', V1], [baseB, 'v2', V2]]) {
    const r = await publish(base, v, MODEL, buf);
    assert.equal(r.status, 201, `发布 ${v} 应成功：${await r.text()}`);
  }
  // 经实例 B 发布的 v2，实例 A 内存中没有，必须能从共享盘惰性加载
  const cross = await fetch(`${baseA}/api/firmware/releases/v2/artifact`);
  assert.equal(cross.status, 200);
  assert.equal(cross.headers.get('etag'), `"${sha(V2)}"`);
  assert.ok(Buffer.from(await cross.arrayBuffer()).equals(V2));
});

test('无竞争首次切换：expectedVersion=null 切到 v0，另一实例立即可读', async () => {
  const r = await switchActive(baseA, MODEL, { releaseVersion: 'v0', expectedVersion: null });
  const body = await r.json();
  assert.equal(r.status, 200, JSON.stringify(body));
  assert.equal(body.activeVersion, 'v0');
  assert.equal(r.headers.get('etag'), `"${sha(V0)}"`);

  // 切换落到实例 A，读取落到实例 B：共享映射必须跨实例一致
  await assertServes(baseB, MODEL, V0, '实例B/v0');
});

test('双实例竞争：同一前置版本 v0 上 A→v1 与 B→v2 至多一个 200，败者 409', async () => {
  const [ra, rb] = await Promise.all([
    switchActive(baseA, MODEL, { releaseVersion: 'v1', expectedVersion: 'v0' }),
    switchActive(baseB, MODEL, { releaseVersion: 'v2', expectedVersion: 'v0' }),
  ]);
  const statuses = [ra.status, rb.status].sort((x, y) => x - y);
  assert.deepEqual(statuses, [200, 409], `竞争结果应为恰一个 200 与一个 409，实际 ${statuses}`);

  const loser = ra.status === 409 ? ra : rb;
  assert.equal((await loser.json()).error.code, 'VERSION_CONFLICT');
  const winnerRes = ra.status === 200 ? ra : rb;
  const winner = (await winnerRes.json()).activeVersion;
  assert.ok(['v1', 'v2'].includes(winner), `获胜版本应为 v1/v2 之一，实际 ${winner}`);
  assert.equal(winnerRes.headers.get('etag'), `"${sha(winner === 'v1' ? V1 : V2)}"`);

  // 败者重试（前置版本已过期）仍 409，且活动版本保持为获胜版本
  const retry = await switchActive(baseB, MODEL, {
    releaseVersion: winner === 'v1' ? 'v2' : 'v1',
    expectedVersion: 'v0',
  });
  assert.equal(retry.status, 409);
  assert.equal((await retry.json()).error.code, 'VERSION_CONFLICT');

  raceWinner = winner;
  currentVersion = winner;
});

test('竞争后跨实例读取：两个实例的稳定地址都解析到同一获胜版本', async () => {
  const winnerBuf = BYTES[raceWinner];
  // 交替访问两个实例，响应正文与 ETag 必须始终属于同一获胜版本
  for (let i = 0; i < 3; i++) {
    await assertServes(baseA, MODEL, winnerBuf, `实例A/${raceWinner}#${i}`);
    await assertServes(baseB, MODEL, winnerBuf, `实例B/${raceWinner}#${i}`);
  }
  // 失败版本的 ETag 不得再出现在稳定地址：If-Range 持败者 ETag 应回退完整 200（获胜版本）
  const loserBuf = raceWinner === 'v1' ? V2 : V1;
  const stale = await fetch(stableUrl(baseB, MODEL), {
    headers: { Range: 'bytes=0-9', 'If-Range': `"${sha(loserBuf)}"` },
  });
  assert.equal(stale.status, 200);
  assert.equal(stale.headers.get('etag'), `"${sha(winnerBuf)}"`);
  assert.ok(Buffer.from(await stale.arrayBuffer()).equals(winnerBuf));
});

test('无竞争后续切换：以获胜版本为前置正常切换，跨实例立即可见', async () => {
  const next = raceWinner === 'v2' ? 'v1' : 'v2'; // 切向尚未激活的另一个版本
  const target = BYTES[next];

  // 前置版本过期（仍持 v0）→ 409，活动版本不变
  const stale = await switchActive(baseA, MODEL, { releaseVersion: next, expectedVersion: 'v0' });
  assert.equal(stale.status, 409);
  assert.equal((await stale.json()).error.code, 'VERSION_CONFLICT');
  const still = await fetch(stableUrl(baseB, MODEL));
  assert.equal(still.headers.get('etag'), `"${sha(BYTES[raceWinner])}"`);
  await still.arrayBuffer();

  // 正确前置版本 → 200；请求落在实例 B，读取落到实例 A
  const ok = await switchActive(baseB, MODEL, { releaseVersion: next, expectedVersion: raceWinner });
  const okBody = await ok.json();
  assert.equal(ok.status, 200, JSON.stringify(okBody));
  assert.equal(okBody.activeVersion, next);
  await assertServes(baseA, MODEL, target, `实例A/${next}`);
  await assertServes(baseB, MODEL, target, `实例B/${next}`);

  currentVersion = next;
});

test('既有规则跨实例保持：发布不可变、型号匹配、expectedVersion 校验', async () => {
  // 重复发布 → 409，原字节不变（经另一实例读取核对）
  const dup = await publish(baseB, 'v1', MODEL, Buffer.from('tampered-v1'));
  assert.equal(dup.status, 409);
  const orig = await fetch(`${baseA}/api/firmware/releases/v1/artifact`);
  assert.ok(Buffer.from(await orig.arrayBuffer()).equals(V1));

  // 型号不符 → 409 MODEL_MISMATCH（发布在 A，切换在 B）
  const other = Buffer.from('wt-other-model-'.repeat(100));
  const pr = await publish(baseA, 'v9-other', 'WT-OTHER-MULTI', other);
  assert.equal(pr.status, 201);
  const mm = await switchActive(baseB, MODEL, { releaseVersion: 'v9-other', expectedVersion: null });
  assert.equal(mm.status, 409);
  assert.equal((await mm.json()).error.code, 'MODEL_MISMATCH');

  // 未知发布 → 404
  const nf = await switchActive(baseA, MODEL, { releaseVersion: 'v-not-exist', expectedVersion: null });
  assert.equal(nf.status, 404);
  assert.equal((await nf.json()).error.code, 'RELEASE_NOT_FOUND');

  // 已切换型号拒绝 expectedVersion=null（首切语义）
  const nullAgain = await switchActive(baseB, MODEL, { releaseVersion: 'v0', expectedVersion: null });
  assert.equal(nullAgain.status, 409);
  assert.equal((await nullAgain.json()).error.code, 'VERSION_CONFLICT');
});

test('跨实例首切竞争：新型号上两个 null 前置请求至多一个成功', async () => {
  const model = 'WT-MULTI-FIRST';
  const d1 = Buffer.from('first-race-a-'.repeat(120));
  const d2 = Buffer.from('first-race-b-'.repeat(120));
  assert.equal((await publish(baseA, 'f1', model, d1)).status, 201);
  assert.equal((await publish(baseB, 'f2', model, d2)).status, 201);

  const results = await Promise.all([
    switchActive(baseA, model, { releaseVersion: 'f1', expectedVersion: null }),
    switchActive(baseB, model, { releaseVersion: 'f2', expectedVersion: null }),
  ]);
  const statuses = results.map((r) => r.status).sort((a, b) => a - b);
  assert.deepEqual(statuses, [200, 409], `首切竞争应恰一个 200，实际 ${statuses}`);
  const bodies = await Promise.all(results.map((r) => r.json()));
  const winner = bodies.find((b) => b.activeVersion).activeVersion;
  const winnerBuf = winner === 'f1' ? d1 : d2;
  await assertServes(baseA, model, winnerBuf, `实例A/${winner}`);
  await assertServes(baseB, model, winnerBuf, `实例B/${winner}`);
});

test('多请求并发竞争：同一前置版本上 6 个跨实例请求恰一个 200', async () => {
  const model = 'WT-MULTI-BURST';
  const bufs = {};
  for (let i = 0; i < 6; i++) {
    bufs[`b${i}`] = Buffer.from(`burst-${i}-`.repeat(150));
    const base = i % 2 === 0 ? baseA : baseB;
    assert.equal((await publish(base, `b${i}`, model, bufs[`b${i}`])).status, 201);
  }
  // 先无竞争首切到 b0
  const first = await switchActive(baseA, model, { releaseVersion: 'b0', expectedVersion: null });
  assert.equal(first.status, 200);

  const race = await Promise.all([1, 2, 3, 4, 5].map((i) => {
    const base = i % 2 === 0 ? baseA : baseB;
    return switchActive(base, model, { releaseVersion: `b${i}`, expectedVersion: 'b0' });
  }));
  const ok = race.filter((r) => r.status === 200);
  const conflict = race.filter((r) => r.status === 409);
  assert.equal(ok.length, 1, `并发竞争应恰一个 200，实际 ${race.map((r) => r.status)}`);
  assert.equal(conflict.length, 4);
  for (const r of conflict) {
    assert.equal((await r.json()).error.code, 'VERSION_CONFLICT');
  }
  const winner = (await ok[0].json()).activeVersion;
  for (const base of [baseA, baseB]) {
    await assertServes(base, model, bufs[winner], `实例/${winner}`);
  }
});

test('实例重启与交替访问：新实例加载同一 DATA_DIR 后活动固件唯一一致', async () => {
  const current = currentVersion;
  const curBuf = BYTES[current];

  // 模拟滚动重启：第三个实例加载同一 DATA_DIR
  const srvC = await startInstance();
  const baseC = `http://127.0.0.1:${srvC.address().port}`;
  try {
    await assertServes(baseC, MODEL, curBuf, `实例C/${current}`);
    // 交替访问三个实例，结果必须一致
    for (const base of [baseA, baseB, baseC, baseA]) {
      const r = await fetch(stableUrl(base, MODEL));
      assert.equal(r.status, 200);
      assert.equal(r.headers.get('etag'), `"${sha(curBuf)}"`);
      assert.ok(Buffer.from(await r.arrayBuffer()).equals(curBuf));
    }
    // 重启实例上的正常切换：以共享盘上的当前版本为前置
    const back = current === 'v2' ? 'v1' : 'v2';
    const backBuf = back === 'v2' ? V2 : V1;
    const sw = await switchActive(baseC, MODEL, { releaseVersion: back, expectedVersion: current });
    assert.equal(sw.status, 200, await sw.text());
    for (const base of [baseA, baseB, baseC]) {
      await assertServes(base, MODEL, backBuf, `重启后/${back}`);
    }
  } finally {
    await stopInstance(srvC);
  }
});
