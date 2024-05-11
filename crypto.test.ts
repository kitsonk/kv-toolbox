import {
  assert,
  assertEquals,
  setup,
  teardown,
  timingSafeEqual,
} from "./_test_util.ts";

import { CryptoKv, generateKey } from "./crypto.ts";

Deno.test({
  name: "encrypt/decrypt blob - Uint8Array",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const value = window.crypto.getRandomValues(new Uint8Array(65_536));
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await cryptoKv.getBlob(["example"]);
    assert(actual.value);
    assertEquals(actual.versionstamp, res.versionstamp);
    assert(timingSafeEqual(actual.value, value));
    return teardown();
  },
});

Deno.test({
  name: "encrypt/decrypt blob - ArrayBuffer",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const value = window.crypto.getRandomValues(new Uint8Array(65_536)).buffer;
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await cryptoKv.getBlob(["example"]);
    assert(actual.value);
    assertEquals(actual.versionstamp, res.versionstamp);
    assert(timingSafeEqual(actual.value, value));
    return teardown();
  },
});

Deno.test({
  name: "setBlob - blob",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const part = window.crypto.getRandomValues(new Uint8Array(65_536));
    const value = new Blob([part], { type: "text/plain" });
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await cryptoKv.getBlob(["example"]);
    assert(actual.value);
    assertEquals(actual.versionstamp, res.versionstamp);
    assert(timingSafeEqual(actual.value, part));
    return teardown();
  },
});

Deno.test({
  name: "setBlob - file",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const part = window.crypto.getRandomValues(new Uint8Array(65_536));
    const value = new File([part], "test.bin", { type: "text/plain" });
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await cryptoKv.getBlob(["example"]);
    assert(actual.value);
    assertEquals(actual.versionstamp, res.versionstamp);
    assert(timingSafeEqual(actual.value, part));
    return teardown();
  },
});

Deno.test({
  name: "getBlob - as Blob",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const part = window.crypto.getRandomValues(new Uint8Array(65_536));
    const value = new File([part], "test.bin", { type: "text/plain" });
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await cryptoKv.getBlob(["example"], { blob: true });
    assert(actual.value);
    assertEquals(actual.versionstamp, res.versionstamp);
    assert(timingSafeEqual(await actual.value.arrayBuffer(), part));
    assert(actual.value instanceof File);
    assert(actual.value.name, "test.bin");
    return teardown();
  },
});

Deno.test({
  name: "getAsBlob",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const part = window.crypto.getRandomValues(new Uint8Array(65_536));
    const value = new File([part], "test.bin", { type: "text/plain" });
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await cryptoKv.getAsBlob(["example"]);
    assert(actual);
    assert(timingSafeEqual(await actual.arrayBuffer(), part));
    assert(actual instanceof File);
    assert(actual.name, "test.bin");
    return teardown();
  },
});

Deno.test({
  name: "getBlobMeta",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const part = window.crypto.getRandomValues(new Uint8Array(65_536));
    const value = new File([part], "test.bin", { type: "text/plain" });
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await cryptoKv.getBlobMeta(["example"]);
    assert(actual.value);
    assertEquals(actual.value.kind, "file");
    assertEquals(actual.value.encrypted, true);
    return teardown();
  },
});

Deno.test({
  name: "deleteBlob",
  async fn() {
    const kv = await setup();
    const key = generateKey();
    const cryptoKv = new CryptoKv(kv, key);
    const part = window.crypto.getRandomValues(new Uint8Array(65_536));
    const value = new File([part], "test.bin", { type: "text/plain" });
    const res = await cryptoKv.setBlob(["example"], value);
    assert(res.ok);
    await cryptoKv.deleteBlob(["example"]);
    let found = false;
    for await (const _ of kv.list({ prefix: ["example"] })) {
      found = true;
    }
    assert(!found);
    return teardown();
  },
});
