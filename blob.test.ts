import {
  assert,
  assertEquals,
  setup,
  teardown,
  timingSafeEqual,
} from "./_test_util.ts";

import {
  get,
  getAsBlob,
  getAsJSON,
  getAsStream,
  remove,
  set,
  toJSON,
  toValue,
} from "./blob.ts";
import { keys } from "./keys.ts";

Deno.test({
  name: "set - sets a blob value",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    window.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
    ]);
    return teardown();
  },
});

Deno.test({
  name: "set - sets a blob value as a stream",
  async fn() {
    const kv = await setup();
    const data = new Uint8Array(65_536);
    window.crypto.getRandomValues(data);
    const blob = new Blob([data]);
    await set(kv, ["hello"], blob.stream());
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
    ]);
    return teardown();
  },
});

Deno.test({
  name: "set - sets a blob value as a blob",
  async fn() {
    const kv = await setup();
    const data = new Uint8Array(65_536);
    window.crypto.getRandomValues(data);
    const blob = new Blob([data], { type: "application/octet-stream" });
    await set(kv, ["hello"], blob);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
      ["hello", "__kv_toolbox_meta__"],
    ]);
    const metaEntry = await kv.get(["hello", "__kv_toolbox_meta__"]);
    assertEquals(metaEntry.value, {
      kind: "blob",
      type: "application/octet-stream",
    });
    return teardown();
  },
});

Deno.test({
  name: "set - sets a blob value as a file",
  async fn() {
    const kv = await setup();
    const data = new Uint8Array(65_536);
    window.crypto.getRandomValues(data);
    const blob = new File([data], "test.bin", {
      type: "application/octet-stream",
      lastModified: 12345678,
    });
    await set(kv, ["hello"], blob);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
      ["hello", "__kv_toolbox_meta__"],
    ]);
    const metaEntry = await kv.get(["hello", "__kv_toolbox_meta__"]);
    assertEquals(metaEntry.value, {
      kind: "file",
      type: "application/octet-stream",
      name: "test.bin",
      lastModified: 12345678,
    });
    return teardown();
  },
});

Deno.test({
  name: "set - replacing value sizes keys properly",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    window.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
    ]);
    const blob2 = blob.slice(0, 1_000);
    await set(kv, ["hello"], blob2);
    const actual2 = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual2, [["hello", "__kv_toolbox_blob__", 1]]);
    return teardown();
  },
});

Deno.test({
  name: "set - large blob",
  async fn() {
    const kv = await setup();
    const blob = await Deno.readFile("./_fixtures/png-1mb.png");
    await set(kv, ["hello"], blob);
    const actual = await get(kv, ["hello"]);
    assert(actual);
    assert(timingSafeEqual(actual, blob));
    return teardown();
  },
});

Deno.test({
  name: "get - assembles blob value as array buffer",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    window.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    const actual = await get(kv, ["hello"]);
    assert(actual);
    assert(timingSafeEqual(actual, blob));
    return teardown();
  },
});

Deno.test({
  name: "get - option stream streams blob value",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    window.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    const stream = get(kv, ["hello"], { stream: true });
    let count = 0;
    for await (const _ of stream) {
      count++;
    }
    assertEquals(count, 2);
    return teardown();
  },
});

Deno.test({
  name: "getAsStream - streams blob value",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    window.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    const stream = getAsStream(kv, ["hello"]);
    let count = 0;
    for await (const _ of stream) {
      count++;
    }
    assertEquals(count, 2);
    return teardown();
  },
});

Deno.test({
  name: "getAsJSON - handles array buffer like",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    window.crypto.getRandomValues(u8);
    await set(kv, ["hello"], u8);
    const json = await getAsJSON(kv, ["hello"]);
    assert(json);
    assertEquals(json.parts.length, 2);
    assertEquals(json.meta, { kind: "buffer" });
    return teardown();
  },
});

Deno.test({
  name: "getAsJSON - handles blob",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    window.crypto.getRandomValues(u8);
    await set(
      kv,
      ["hello"],
      new Blob([u8], { type: "application/octet-stream" }),
    );
    const json = await getAsJSON(kv, ["hello"]);
    assert(json);
    assertEquals(json.parts.length, 2);
    assertEquals(json.meta, { kind: "blob", type: "application/octet-stream" });
    return teardown();
  },
});

Deno.test({
  name: "getAsJSON - handles file",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    window.crypto.getRandomValues(u8);
    await set(
      kv,
      ["hello"],
      new File([u8], "test.bin", {
        type: "application/octet-stream",
        lastModified: 1711349710546,
      }),
    );
    const json = await getAsJSON(kv, ["hello"]);
    assert(json);
    assertEquals(json.parts.length, 2);
    assertEquals(json.meta, {
      kind: "file",
      type: "application/octet-stream",
      name: "test.bin",
      lastModified: 1711349710546,
    });
    return teardown();
  },
});

Deno.test({
  name: "toJSON/toValue - File",
  async fn() {
    const u8 = new Uint8Array(65_536);
    window.crypto.getRandomValues(u8);
    const json = await toJSON(
      new File([u8], "test.bin", {
        type: "application/octet-stream",
        lastModified: 1711349710546,
      }),
    );
    assertEquals(json.meta, {
      kind: "file",
      type: "application/octet-stream",
      name: "test.bin",
      lastModified: 1711349710546,
    });
    assertEquals(json.parts.length, 2);
    const value = toValue(json);
    assert(value instanceof File);
    assertEquals((await value.arrayBuffer()).byteLength, 65_536);
    assertEquals(value.name, "test.bin");
    assertEquals(value.lastModified, 1711349710546);
    assertEquals(value.type, "application/octet-stream");
  },
});

Deno.test({
  name: "toJSON/toValue - Blob",
  async fn() {
    const u8 = new Uint8Array(65_536);
    window.crypto.getRandomValues(u8);
    const json = await toJSON(
      new Blob([u8], { type: "application/octet-stream" }),
    );
    assertEquals(json.meta, { kind: "blob", type: "application/octet-stream" });
    assertEquals(json.parts.length, 2);
    const value = toValue(json);
    assert(value instanceof Blob);
    assert(!(value instanceof File));
    assertEquals((await value.arrayBuffer()).byteLength, 65_536);
    assertEquals(value.type, "application/octet-stream");
  },
});

Deno.test({
  name: "toJSON/toValue - buffer",
  async fn() {
    const u8 = new Uint8Array(65_536);
    window.crypto.getRandomValues(u8);
    const json = await toJSON(u8);
    assertEquals(json.meta, { kind: "buffer" });
    assertEquals(json.parts.length, 2);
    const value = toValue(json);
    assert(value instanceof Uint8Array);
    assertEquals(value.byteLength, 65_536);
  },
});

Deno.test({
  name: "set/get - files",
  async fn() {
    const kv = await setup();
    const data = await Deno.readFile("./_fixtures/png-1mb.png");
    const stats = await Deno.stat("./_fixtures/png-1mb.png");
    const file = new File([data], "png-1mb.png", {
      lastModified: stats.mtime?.getTime(),
      type: "image/png",
    });
    await set(kv, ["hello"], file);
    const actual = await get(kv, ["hello"], { blob: true });
    assert(actual instanceof File);
    assertEquals(actual.name, "png-1mb.png");
    assertEquals(actual.type, "image/png");
    assertEquals(actual.lastModified, file.lastModified);
    assert(
      timingSafeEqual(await actual.arrayBuffer(), await file.arrayBuffer()),
    );
    return teardown();
  },
});

Deno.test({
  name: "set/getAsBlob - files",
  async fn() {
    const kv = await setup();
    const data = await Deno.readFile("./_fixtures/png-1mb.png");
    const stats = await Deno.stat("./_fixtures/png-1mb.png");
    const file = new File([data], "png-1mb.png", {
      lastModified: stats.mtime?.getTime(),
      type: "image/png",
    });
    await set(kv, ["hello"], file);
    const actual = await getAsBlob(kv, ["hello"]);
    assert(actual instanceof File);
    assertEquals(actual.name, "png-1mb.png");
    assertEquals(actual.type, "image/png");
    assertEquals(actual.lastModified, file.lastModified);
    assert(
      timingSafeEqual(await actual.arrayBuffer(), await file.arrayBuffer()),
    );
    return teardown();
  },
});

Deno.test({
  name: "remove - deletes a blob value",
  async fn() {
    const kv = await setup();
    const data = new Uint8Array(65_536);
    window.crypto.getRandomValues(data);
    await set(kv, ["hello"], new Blob([data]));
    assertEquals((await keys(kv, { prefix: ["hello"] })).length, 3);
    await remove(kv, ["hello"]);
    assertEquals((await keys(kv, { prefix: ["hello"] })).length, 0);
    return teardown();
  },
});

Deno.test({
  name: "remove - non-existent key is a noop",
  async fn() {
    const kv = await setup();
    await remove(kv, ["hello"]);
    return teardown();
  },
});
