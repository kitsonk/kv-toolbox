import { assert, assertEquals, setup, teardown } from "./test_util.ts";

import { keys } from "./keys.ts";

Deno.test({
  name: "keys - returns a list of keys",
  async fn() {
    const kv = await setup();
    const res = await kv.atomic()
      .set(["a"], "a")
      .set(["a", "b"], "b")
      .set(["a", "b", "c"], "c")
      .set(["d"], "d")
      .commit();
    assert(res.ok);

    const actual = await keys(kv, { prefix: ["a"] });

    assertEquals(actual, [["a", "b"], ["a", "b", "c"]]);
    return teardown();
  },
});
