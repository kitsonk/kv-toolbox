import {
  assert,
  assertEquals,
  assertRejects,
  setup,
  teardown,
  timingSafeEqual,
} from "./_test_util.ts";

import {
  get,
  getAsBlob,
  getAsJSON,
  getAsResponse,
  getAsStream,
  getMeta,
  remove,
  set,
  toBlob,
  toJSON,
  toValue,
} from "./blob.ts";
import { keys } from "./keys.ts";

Deno.test({
  name: "set - sets a blob value",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(blob);
    const res = await set(kv, ["hello"], blob);
    assert(res.ok);
    assert(res.versionstamp);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
      ["hello", "__kv_toolbox_meta__"],
    ]);
    return teardown();
  },
});

Deno.test({
  name: "set - sets a DataView value",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(u8);
    const blob = new DataView(u8.buffer);
    const res = await set(kv, ["hello"], blob);
    assert(res.ok);
    assert(res.versionstamp);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
      ["hello", "__kv_toolbox_meta__"],
    ]);
    return teardown();
  },
});

Deno.test({
  name: "set - sets a blob value as a stream",
  async fn() {
    const kv = await setup();
    const data = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(data);
    const blob = new Blob([data]);
    const res = await set(kv, ["hello"], blob.stream());
    assert(res.ok);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
      ["hello", "__kv_toolbox_meta__"],
    ]);
    return teardown();
  },
});

Deno.test({
  name: "set - sets a blob value as a blob",
  async fn() {
    const kv = await setup();
    const data = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(data);
    const blob = new Blob([data], { type: "application/octet-stream" });
    const res = await set(kv, ["hello"], blob);
    assert(res.ok);
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
      size: 65_536,
    });
    return teardown();
  },
});

Deno.test({
  name: "set - sets a blob value as a file",
  async fn() {
    const kv = await setup();
    const data = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(data);
    const blob = new File([data], "test.bin", {
      type: "application/octet-stream",
      lastModified: 12345678,
    });
    const res = await set(kv, ["hello"], blob);
    assert(res.ok);
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
      size: 65_536,
    });
    return teardown();
  },
});

Deno.test({
  name: "set - replacing value sizes keys properly",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(blob);
    const res = await set(kv, ["hello"], blob);
    assert(res.ok);
    const actual = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_blob__", 2],
      ["hello", "__kv_toolbox_meta__"],
    ]);
    const blob2 = blob.slice(0, 1_000);
    await set(kv, ["hello"], blob2);
    const actual2 = await keys(kv, { prefix: ["hello"] });
    assertEquals(actual2, [
      ["hello", "__kv_toolbox_blob__", 1],
      ["hello", "__kv_toolbox_meta__"],
    ]);
    return teardown();
  },
});

Deno.test({
  name: "set - large blob",
  async fn() {
    const kv = await setup();
    const blob = await Deno.readFile("./_fixtures/png-1mb.png");
    const res = await set(kv, ["hello"], blob);
    assert(res.ok);
    const actual = await get(kv, ["hello"]);
    assert(actual.value);
    assert(timingSafeEqual(actual.value, blob));
    return teardown();
  },
});

Deno.test({
  name: "set - very large blob",
  async fn() {
    const kv = await setup();
    const blob = await Deno.readFile("./_fixtures/mp4-7mb.mp4");
    const res = await set(kv, ["hello"], blob);
    assert(res.ok);
    const actual = await get(kv, ["hello"]);
    assert(actual.value);
    assert(timingSafeEqual(actual.value, blob));
    return teardown();
  },
});

Deno.test({
  name: "set - rejects TypeError with invalid value",
  async fn() {
    const kv = await setup();
    const blob = "kv-toolbox".repeat(1000);
    await assertRejects(
      // @ts-ignore to make the test type check
      () => set(kv, ["hello"], blob),
      "Blob must be typed array, array buffer, ReadableStream, Blob, or File",
    );
    return teardown();
  },
});

Deno.test({
  name: "get - assembles blob value as array buffer",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    const actual = await get(kv, ["hello"]);
    assert(actual.value);
    assert(timingSafeEqual(actual.value, blob));
    return teardown();
  },
});

Deno.test({
  name: "get - option stream streams blob value",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    const entry = await get(kv, ["hello"], { stream: true });
    assert(entry.value);
    let count = 0;
    for await (const _ of entry.value) {
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
    globalThis.crypto.getRandomValues(blob);
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
  name: "getAsResponse - set response correctly",
  async fn() {
    const kv = await setup();
    const blob = new Blob(
      [`<DOCTYPE! html><html><body>Hello!</body></html>`],
      { type: "text/html" },
    );
    await set(kv, ["index.html"], blob);
    const actual = await getAsResponse(kv, ["index.html"]);
    assertEquals(actual.headers.get("content-type"), "text/html");
    assertEquals(actual.headers.get("content-length"), "47");
    assertEquals([...actual.headers].length, 2);
    assertEquals(actual.status, 200);
    assertEquals(actual.statusText, "OK");
    assertEquals(
      await actual.text(),
      `<DOCTYPE! html><html><body>Hello!</body></html>`,
    );
    return teardown();
  },
});

Deno.test({
  name: "getAsResponse - missing entry",
  async fn() {
    const kv = await setup();
    const actual = await getAsResponse(kv, ["index.html"]);
    assertEquals([...actual.headers].length, 0);
    assertEquals(actual.status, 404);
    assertEquals(actual.statusText, "Not Found");
    return teardown();
  },
});

Deno.test({
  name: "getAsResponse - missing entry uses options",
  async fn() {
    const kv = await setup();
    const actual = await getAsResponse(kv, ["index.html"], {
      notFoundBody: "not found",
      notFoundHeaders: { "content-type": "text/plain" },
    });
    assertEquals(actual.headers.get("content-type"), "text/plain");
    assertEquals([...actual.headers].length, 1);
    assertEquals(actual.status, 404);
    assertEquals(actual.statusText, "Not Found");
    assertEquals(await actual.text(), "not found");
    return teardown();
  },
});

Deno.test({
  name: "getAsResponse - processes headers init",
  async fn() {
    const kv = await setup();
    const blob = new Blob(
      [`<DOCTYPE! html><html><body>Hello!</body></html>`],
      { type: "text/html" },
    );
    await set(kv, ["index.html"], blob);
    const actual = await getAsResponse(kv, ["index.html"], {
      headers: { "X-KV-Toolbox": "custom" },
    });
    assertEquals(actual.headers.get("content-type"), "text/html");
    assertEquals(actual.headers.get("content-length"), "47");
    assertEquals(actual.headers.get("x-kv-toolbox"), "custom");
    assertEquals([...actual.headers].length, 3);
    return teardown();
  },
});

Deno.test({
  name: "getAsResponse - contentDisposition is true",
  async fn() {
    const kv = await setup();
    const data = await Deno.readFile("./_fixtures/png-1mb.png");
    const stats = await Deno.stat("./_fixtures/png-1mb.png");
    const file = new File([data], "png-1mb.png", {
      lastModified: stats.mtime?.getTime(),
      type: "image/png",
    });
    await set(kv, ["hello"], file);
    const actual = await getAsResponse(kv, ["hello"], {
      contentDisposition: true,
    });
    assertEquals(actual.headers.get("content-type"), "image/png");
    assertEquals(actual.headers.get("content-length"), "1050986");
    assertEquals(
      actual.headers.get("content-disposition"),
      'attachment; filename="png-1mb.png"',
    );
    assertEquals([...actual.headers].length, 3);
    return teardown();
  },
});

Deno.test({
  name: "getAsJSON - handles array buffer like",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(u8);
    await set(kv, ["hello"], u8);
    const json = await getAsJSON(kv, ["hello"]);
    assert(json);
    assertEquals(json.parts.length, 2);
    assertEquals(json.meta, { kind: "buffer", size: 65_536 });
    return teardown();
  },
});

Deno.test({
  name: "getAsJSON - handles blob",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(u8);
    await set(
      kv,
      ["hello"],
      new Blob([u8], { type: "application/octet-stream" }),
    );
    const json = await getAsJSON(kv, ["hello"]);
    assert(json);
    assertEquals(json.parts.length, 2);
    assertEquals(json.meta, {
      kind: "blob",
      type: "application/octet-stream",
      size: 65_536,
    });
    return teardown();
  },
});

Deno.test({
  name: "getAsJSON - handles file",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(u8);
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
      size: 65_536,
    });
    return teardown();
  },
});

Deno.test({
  name: "getMeta",
  async fn() {
    const kv = await setup();
    const u8 = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(u8);
    await set(kv, ["hello"], u8);
    const meta = await getMeta(kv, ["hello"]);
    assert(meta);
    assertEquals(meta.value, { kind: "buffer", size: 65_536 });
    return teardown();
  },
});

Deno.test({
  name: "toJSON/toValue - File",
  async fn() {
    const u8 = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(u8);
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
    globalThis.crypto.getRandomValues(u8);
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
    globalThis.crypto.getRandomValues(u8);
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
    assert(actual.value instanceof File);
    assertEquals(actual.value.name, "png-1mb.png");
    assertEquals(actual.value.type, "image/png");
    assertEquals(actual.value.lastModified, file.lastModified);
    assert(
      timingSafeEqual(
        await actual.value.arrayBuffer(),
        await file.arrayBuffer(),
      ),
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
    globalThis.crypto.getRandomValues(data);
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

Deno.test({
  name: "toBlob() - default type",
  async fn() {
    const actual = toBlob("some sort of string");
    assertEquals(actual.type, "text/plain");
    assertEquals(await actual.text(), "some sort of string");
  },
});

Deno.test({
  name: "toBlob() - provided type",
  fn() {
    const actual = toBlob("some sort of string", "text/html");
    assertEquals(actual.type, "text/html");
  },
});
