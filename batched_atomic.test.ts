import {
  assert,
  assertEquals,
  assertNotEquals,
  delay,
  setup,
  teardown,
} from "./_test_util.ts";
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
    window.crypto.getRandomValues(blob);
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
    window.crypto.getRandomValues(blob);
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
