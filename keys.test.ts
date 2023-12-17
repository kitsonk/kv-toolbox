import { assert, assertEquals, setup, teardown } from "./_test_util.ts";

import { equals, keys, startsWith, unique, uniqueCount } from "./keys.ts";

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

Deno.test({
  name: "uniqueCount - returns a list of unique sub-keys",
  async fn() {
    const kv = await setup();
    const res = await kv.atomic()
      .set(["a"], "a")
      .set(["a", "b"], "b")
      .set(["a", "b", "c"], "c")
      .set(["a", "d", "f", "g"], "g")
      .set(["a", "h"], "h")
      .set(["e"], "e")
      .commit();
    assert(res.ok);

    const actual = await uniqueCount(kv, ["a"]);

    assertEquals(actual, [
      { key: ["a", "b"], count: 1 },
      { key: ["a", "d"], count: 1 },
      { key: ["a", "h"], count: 0 },
    ]);
    return teardown();
  },
});

Deno.test({
  name: "uniqueCount - handles Uint8Array equality",
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

    const actual = await uniqueCount(kv, ["a"]);

    assertEquals(actual, [
      { key: ["a", new Uint8Array([2, 3, 4])], count: 1 },
      { key: ["a", new Uint8Array([4, 5, 6])], count: 1 },
    ]);
    return teardown();
  },
});

Deno.test({
  name: "equals",
  fn() {
    assert(equals(["a"], ["a"]));
    assert(!equals(["a"], ["b"]));
    assert(equals([1], [1]));
    assert(!equals([1], ["1"]));
    assert(
      equals(["a", 1, 1n, true, new Uint8Array([1, 2, 3])], [
        "a",
        1,
        1n,
        true,
        new Uint8Array([1, 2, 3]),
      ]),
    );
    assert(!equals(["a", 1n, 1, true], ["a", 1, 1n, true]));
    assert(
      !equals(["a", 1, 1n, true, new Uint8Array([3, 2, 1])], [
        "a",
        1,
        1n,
        true,
        new Uint8Array([1, 2, 3]),
      ]),
    );
  },
});

Deno.test({
  name: "startsWith",
  fn() {
    assert(startsWith(["a", "b"], ["a"]));
    assert(startsWith(["a", "b"], ["a", "b"]));
    assert(!startsWith(["a"], ["a", "b"]));
    assert(
      startsWith(["a", new Uint8Array([1, 2, 3]), 1, 1n, true], [
        "a",
        new Uint8Array([1, 2, 3]),
      ]),
    );
    assert(
      !startsWith(["a", new Uint8Array([1, 2, 3]), 1, 1n, true], [
        "a",
        new Uint8Array([1, 2, 3]),
        1,
        2n,
      ]),
    );
  },
});
