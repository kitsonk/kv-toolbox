import {
  assert,
  assertEquals,
  setup,
  teardown,
  timingSafeEqual,
} from "./_test_util.ts";

import { get, remove, set } from "./blob.ts";
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
  name: "remove - deletes a blob value",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    window.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    assertEquals((await keys(kv, { prefix: ["hello"] })).length, 2);
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
