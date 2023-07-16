import {
  assert,
  assertEquals,
  assertNotEquals,
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
