import { assert, assertEquals, assertNotEquals, delay, setup, teardown } from "./_test_util.ts";
import { keys } from "./keys.ts";

import { batchedAtomic } from "./batched_atomic.ts";
import { set } from "./blob.ts";

Deno.test({
  name: "batched atomic handles checks",
  async fn() {
    const kv = await setup();
    const res = await kv.set(["hello"], "world");
    assert(res.ok);
    const { versionstamp } = res;
    const operation = batchedAtomic(kv);
    operation.check({ key: ["hello"], versionstamp });
    operation.set(["hello"], "deno kv");
    const actual = await operation.commit();
    assertEquals(actual.length, 1);
    assert(actual[0].ok);
    assertNotEquals(actual[0].versionstamp, versionstamp);
    return teardown();
  },
});

Deno.test({
  name: "batched atomic handles blob checks",
  async fn() {
    const kv = await setup();
    let value = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(value);
    const res = await set(kv, ["hello"], value);
    assert(res.ok);
    value = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(value);
    const actual = await batchedAtomic(kv)
      .checkBlob({ key: ["hello"], versionstamp: res.versionstamp })
      .setBlob(["hello"], value)
      .commit();
    assertEquals(actual.length, 1);
    assert(actual[0].ok);
    assert(actual[0].versionstamp !== res.versionstamp);
    return teardown();
  },
});

Deno.test({
  name: "batched atomic handles blob check fail",
  async fn() {
    const kv = await setup();
    let value = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(value);
    const res = await set(kv, ["hello"], value);
    assert(res.ok);
    value = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(value);
    const actual = await batchedAtomic(kv)
      .checkBlob({ key: ["hello"], versionstamp: null })
      .setBlob(["hello"], value)
      .commit();
    assertEquals(actual.length, 1);
    assert(!actual[0].ok);
    return teardown();
  },
});

Deno.test({
  name: "batch atomic deals with big transactions",
  async fn() {
    const kv = await setup();
    const op = batchedAtomic(kv);
    for (let i = 0; i < 4000; i++) {
      op.set([i.toString().repeat(500)], i.toString().repeat(4000));
    }
    const actual = await op.commit();
    assertEquals(actual.length, 99);
    assert(actual.every(({ ok }) => ok));
    return teardown();
  },
});

Deno.test({
  name: "batched atomic handles failed check",
  async fn() {
    const kv = await setup();
    const res = await kv.set(["hello"], "world");
    assert(res.ok);
    const operation = batchedAtomic(kv);
    operation.check({ key: ["hello"], versionstamp: null });
    operation.set(["hello"], "deno kv");
    const actual = await operation.commit();
    assertEquals(actual.length, 1);
    assert(!actual[0].ok);
    return teardown();
  },
});

Deno.test({
  name: "batched atomic supports expiresIn option",
  ignore: true, // not currently working in Deno CLI
  async fn() {
    const kv = await setup();
    const operation = batchedAtomic(kv);
    operation.set(["hello"], "deno kv", { expireIn: 100 });
    const actual = await operation.commit();
    assertEquals(actual.length, 1);
    assert(actual[0].ok);
    const res1 = await kv.get<string>(["hello"]);
    assertEquals(res1.value, "deno kv");
    await delay(700);
    const res2 = await kv.get<string>(["hello"]);
    assertEquals(res2.value, null);
    return teardown();
  },
});

Deno.test({
  name: "batched atomic supports setting blobs",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(blob);
    const operation = batchedAtomic(kv);
    operation.setBlob(["hello"], blob);
    await operation.commit();
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
  name: "batched atomic supports deleting blobs",
  async fn() {
    const kv = await setup();
    const blob = new Uint8Array(65_536);
    globalThis.crypto.getRandomValues(blob);
    await set(kv, ["hello"], blob);
    assertEquals((await keys(kv, { prefix: ["hello"] })).length, 3);
    const operation = batchedAtomic(kv);
    await operation
      .deleteBlob(["hello"])
      .commit();
    assertEquals((await keys(kv, { prefix: ["hello"] })).length, 0);
    return teardown();
  },
});

Deno.test({
  name: "batched atomic mutate handles many items",
  async fn() {
    const kv = await setup();
    const items = Array
      .from({ length: 1000 }, (_, index) => index)
      .map((i) => ({
        key: [i],
        value: "x".repeat(2000),
        type: "set" as const,
      }));
    const op = batchedAtomic(kv);
    op.mutate(...items);
    const actual = await op.commit();
    assert(actual.every(({ ok }) => ok));
    return teardown();
  },
});
