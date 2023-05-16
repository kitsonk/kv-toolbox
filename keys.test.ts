import { assert, assertEquals, setup, teardown } from "./_test_util.ts";

import { keys, unique } from "./keys.ts";

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

Deno.test({
  name: "unique - returns a list of unique sub-keys",
  async fn() {
    const kv = await setup();
    const res = await kv.atomic()
      .set(["a"], "a")
      .set(["a", "b"], "b")
      .set(["a", "b", "c"], "c")
      .set(["a", "d", "f", "g"], "g")
      .set(["e"], "e")
      .commit();
    assert(res.ok);

    const actual = await unique(kv, ["a"]);

    assertEquals(actual, [["a", "b"], ["a", "d"]]);
    return teardown();
  },
});

Deno.test({
  name: "unique - handles Uint8Array equality",
  async fn() {
    const kv = await setup();
    const res = await kv.atomic()
      .set(["a"], "a")
      .set(["a", new Uint8Array([2, 3, 4])], "b")
      .set(["a", new Uint8Array([2, 3, 4]), "c"], "c")
      .set(["a", new Uint8Array([4, 5, 6]), "c"], "c")
      .set(["e"], "e")
      .commit();
    assert(res.ok);

    const actual = await unique(kv, ["a"]);

    assertEquals(actual, [
      ["a", new Uint8Array([2, 3, 4])],
      ["a", new Uint8Array([4, 5, 6])],
    ]);
    return teardown();
  },
});
