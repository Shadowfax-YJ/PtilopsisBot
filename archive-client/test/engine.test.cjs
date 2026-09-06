'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const { Engine } = require('../src/engine.cjs');
const { delay } = require('../src/quark.cjs');
const vendorDir = path.join(__dirname, '..', 'vendor', `${process.platform}-${process.arch}`);
const haveVendor = require('node:fs').existsSync(path.join(vendorDir, 'openlist' + (process.platform === 'win32' ? '.exe' : '')));

test('real embedded server and rclone preserve old content; quit kills children', { skip: !haveVendor, timeout: 60000 }, async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'archive-engine-'));
  const source = path.join(root, 'source'), target = path.join(root, 'target');
  await fs.mkdir(path.join(source, 'nested'), { recursive: true }); await fs.mkdir(target);
  await fs.writeFile(path.join(source, 'old.txt'), 'cloud changed');
  await fs.writeFile(path.join(target, 'old.txt'), 'must remain local');
  await fs.writeFile(path.join(source, 'nested', 'new.txt'), 'new bytes');
  const originalTime = (await fs.stat(path.join(target, 'old.txt'))).mtimeMs;
  const engine = new Engine({ dataDir: path.join(root, 'data'), vendorDir, update() {}, async persist() {}, notify() {} });
  t.after(async () => { await engine.close(); await fs.rm(root, { recursive: true, force: true }); });
  await engine.start();
  await engine.localApi('/api/admin/storage/create', { mount_path: '/fixture', driver: 'Local', order: 0, cache_expiration: 0,
    addition: JSON.stringify({ root_folder_path: source, thumbnail: false, show_hidden: false }) });
  const manifest = path.join(root, 'files.txt'); await fs.writeFile(manifest, 'old.txt\nnested/new.txt\n');
  const password = await engine.command('rclone', ['obscure', '-'], { stdio: ['pipe', 'pipe', 'pipe'], inputPassword: engine.password });
  await engine.copy({ id: 'fixture', destination: target }, '/fixture', manifest, password, new AbortController().signal);
  assert.equal(await fs.readFile(path.join(target, 'old.txt'), 'utf8'), 'must remain local');
  assert.equal((await fs.stat(path.join(target, 'old.txt'))).mtimeMs, originalTime);
  assert.equal(await fs.readFile(path.join(target, 'nested', 'new.txt'), 'utf8'), 'new bytes');
  const server = engine.server; await engine.close(); assert(server.exitCode !== null || server.signalCode !== null); assert.equal(engine.children.size, 0);
});

test('stopping a real copy aborts it and never exposes an incomplete final file', { skip: !haveVendor, timeout: 60000 }, async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'archive-cancel-'));
  const source = path.join(root, 'source'), target = path.join(root, 'target');
  await fs.mkdir(source); await fs.mkdir(target);
  await fs.writeFile(path.join(source, 'big.bin'), Buffer.alloc(2 * 1024 * 1024, 71));
  const engine = new Engine({ dataDir: path.join(root, 'data'), vendorDir, update() {}, async persist() {}, notify() {} });
  t.after(async () => { await engine.close(); await fs.rm(root, { recursive: true, force: true }); });
  await engine.start();
  await engine.localApi('/api/admin/storage/create', { mount_path: '/fixture', driver: 'Local', order: 0, cache_expiration: 0,
    addition: JSON.stringify({ root_folder_path: source, thumbnail: false, show_hidden: false }) });
  const manifest = path.join(root, 'files.txt'); await fs.writeFile(manifest, 'big.bin\n');
  const password = await engine.command('rclone', ['obscure', '-'], { stdio: ['pipe', 'pipe', 'pipe'], inputPassword: engine.password });
  const originalSpawn = engine.spawn.bind(engine);
  engine.spawn = (name, args, options) => originalSpawn(name, name === 'rclone' && args[0] === 'copy' ? [...args, '--bwlimit', '64k'] : args, options);
  const controller = new AbortController();
  const copying = engine.copy({ id: 'fixture', destination: target }, '/fixture', manifest, password, controller.signal);
  const rejected = assert.rejects(copying, { name: 'AbortError' });
  await delay(1200); controller.abort(); await rejected;
  assert.equal(require('node:fs').existsSync(path.join(target, 'big.bin')), false);
  engine.spawn = originalSpawn;
  await engine.copy({ id: 'fixture', destination: target }, '/fixture', manifest, password, new AbortController().signal);
  assert.deepEqual(await fs.readFile(path.join(target, 'big.bin')), await fs.readFile(path.join(source, 'big.bin')));
});
