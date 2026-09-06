'use strict';
// Local verification only. Credentials are captured into this process's memory;
// they are never printed, written into the source tree, or packaged.
const { execFileSync } = require('node:child_process');
const { CookieJar } = require('tough-cookie');
const fs = require('node:fs/promises');
const path = require('node:path');
const { Quark } = require('../src/quark.cjs');
const { Engine } = require('../src/engine.cjs');
const { prepareFiles } = require('../src/model.cjs');
async function main() {
  const legacy = process.env.ARCHIVE_LEGACY_SESSION, python = process.env.ARCHIVE_TEST_PYTHON;
  const fid = process.env.ARCHIVE_TEST_FOLDER_ID, target = process.env.ARCHIVE_TEST_DESTINATION;
  if (!legacy || !python || !fid || !target) throw new Error('Explicit local test environment required');
  const code = "import sys,json,win32crypt;sys.stdout.reconfigure(encoding='utf-8');print(win32crypt.CryptUnprotectData(open(sys.argv[1],'rb').read(),None,None,None,0)[1].decode())";
  const cookies = JSON.parse(execFileSync(python, ['-c', code, legacy], { encoding: 'utf8', windowsHide: true }));
  const jar = new CookieJar();
  for (const cookie of cookies) {
    const domain = cookie.domain.replace(/^\./, '');
    await jar.setCookie(`${cookie.name}=${cookie.value}; Domain=${cookie.domain}; Path=${cookie.path || '/'}${cookie.secure ? '; Secure' : ''}`, `https://${domain}/`);
  }
  const quark = new Quark(jar.serializeSync());
  const plan = await prepareFiles(quark, fid, target);
  console.log(JSON.stringify({ liveListing: true, existingSkipped: plan.skipped, newFiles: plan.files.length, newBytes: plan.totalBytes }));
  // Exercise the real Quark -> embedded OpenList -> rclone path using a single
  // small file from the authorized directory, into an isolated test directory.
  let sample;
  async function walk(parent, rel = '') {
    for (const item of await quark.list(parent)) {
      const name = rel ? rel + '/' + item.file_name : item.file_name;
      if (item.file === false || item.dir === true) { if (!sample) await walk(item.fid, name); }
      else if (!sample && item.size > 0 && item.size < 5 * 1024 * 1024) sample = { path: name, size: item.size };
      if (sample) return;
    }
  }
  await walk(fid); if (!sample) throw new Error('No small sample available');
  const root = await fs.mkdtemp(path.join(require('node:os').tmpdir(), 'archive-live-'));
  const destination = path.join(root, 'download'); await fs.mkdir(destination);
  const engine = new Engine({ dataDir: path.join(root, 'profile'), vendorDir: path.join(__dirname, '..', 'vendor', `${process.platform}-${process.arch}`), quark, update() {}, async persist() {}, notify() {} });
  try {
    const mount = await engine.mount({ id: 'live-check' }, fid);
    const manifest = path.join(root, 'files.txt'); await fs.writeFile(manifest, sample.path + '\n');
    const password = await engine.command('rclone', ['obscure', '-'], { stdio: ['pipe', 'pipe', 'pipe'], inputPassword: engine.password });
    await engine.copy({ id: 'live-check', destination }, mount, manifest, password, new AbortController().signal);
    const stat = await fs.stat(path.join(destination, ...sample.path.split('/')));
    if (stat.size !== sample.size) throw new Error('Downloaded size mismatch');
    const hash = require('node:crypto').createHash('sha256').update(await fs.readFile(path.join(destination, ...sample.path.split('/')))).digest('hex');
    const original = path.join(target, ...sample.path.split('/'));
    const originalHash = require('node:crypto').createHash('sha256').update(await fs.readFile(original)).digest('hex');
    if (hash !== originalHash) throw new Error('Downloaded bytes differ from existing archive sample');
    console.log(JSON.stringify({ liveDownload: true, bytes: stat.size, matchesLocalSha256: true }));
  } finally { await engine.close(); await fs.rm(root, { recursive: true, force: true }); }
}
main().catch(err => { console.error(err.message); process.exitCode = 1; });
