import {
  assert,
  assertEquals,
  assertNotEquals,
  delay,
  setup,
  teardown,
} from "./_test_util.ts";

import { batchedAtomic } from "./batchedAtomic.ts";

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
