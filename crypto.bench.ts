import { get, set } from "./blob.ts";
import { CryptoKv, generateKey } from "./crypto.ts";
import { setup } from "./_test_util.ts";

const kv = await setup();

const fixture = globalThis.crypto.getRandomValues(new Uint8Array(65_536));
const encryptionKey128 = generateKey(128);
const cryptoKv128 = new CryptoKv(kv, encryptionKey128);
const encryptionKey192 = generateKey(192);
const cryptoKv192 = new CryptoKv(kv, encryptionKey192);
const encryptionKey256 = generateKey();
const cryptoKv256 = new CryptoKv(kv, encryptionKey256);

Deno.bench({
  name: "standard blob",
  async fn() {
    await set(kv, ["blob"], fixture);
    await get(kv, ["blob"]);
  },
});

Deno.bench({
  name: "encrypted blob AES-128",
  async fn() {
    await cryptoKv128.setBlob(["encrypted128"], fixture);
    await cryptoKv128.getBlob(["encrypted128"]);
  },
});

Deno.bench({
  name: "encrypted blob AES-192",
  async fn() {
    await cryptoKv192.setBlob(["encrypted192"], fixture);
    await cryptoKv192.getBlob(["encrypted192"]);
  },
});

Deno.bench({
  name: "encrypted blob AES-256",
  async fn() {
    await cryptoKv256.setBlob(["encrypted256"], fixture);
    await cryptoKv256.getBlob(["encrypted256"]);
  },
});
