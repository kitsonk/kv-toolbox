// Copyright 2024 the Deno authors. All rights reserved. MIT license.

import { assertAlmostEquals, assertEquals } from "@std/assert";

import { estimateSize } from "./estimate_size.ts";

Deno.test({
  name: "estimateSize - string",
  fn() {
    assertEquals(estimateSize("abcdefghijklmnopq"), 21);
    assertEquals(estimateSize("🥟🥟"), 12);
  },
});

Deno.test({
  name: "estimateSize - number",
  fn() {
    assertEquals(estimateSize(63), 4);
    assertEquals(estimateSize(64), 5);
    assertEquals(estimateSize(8_191), 5);
    assertEquals(estimateSize(8_192), 6);
    assertEquals(estimateSize(1_048_575), 6);
    assertEquals(estimateSize(1_048_576), 7);
    assertEquals(estimateSize(134_217_727), 7);
    assertEquals(estimateSize(134_217_728), 8);
    assertEquals(estimateSize(2_147_483_647), 8);
    assertEquals(estimateSize(2_147_483_648), 11);
    assertEquals(estimateSize(Number.MAX_SAFE_INTEGER), 11);
  },
});

Deno.test({
  name: "estimateSize - boolean",
  fn() {
    assertEquals(estimateSize(true), 3);
    assertEquals(estimateSize(false), 3);
  },
});

Deno.test({
  name: "estimateSize - bigint",
  fn() {
    assertEquals(estimateSize(63n), 12);
    assertEquals(estimateSize(BigInt(Number.MAX_SAFE_INTEGER + 1)), 12);
  },
});

Deno.test({
  name: "estimateSize - undefined",
  fn() {
    assertEquals(estimateSize(undefined), 3);
  },
});

Deno.test({
  name: "estimateSize - null",
  fn() {
    assertEquals(estimateSize(null), 3);
  },
});

Deno.test({
  name: "estimateSize - Date",
  fn() {
    assertEquals(estimateSize(new Date()), 11);
  },
});

Deno.test({
  name: "estimateSize - RegExp",
  fn() {
    assertEquals(estimateSize(/ab[cdefg]hijklmnopq/ig), 25);
  },
});

Deno.test({
  name: "estimateSize - Error",
  fn() {
    assertAlmostEquals(
      estimateSize(new URIError("boo hoo", { cause: new Error("boo") })),
      496,
      100,
    );
  },
});

Deno.test({
  name: "estimateSize - Uint8Array",
  fn() {
    assertEquals(estimateSize(new Uint8Array([1, 2, 3])), 12);
  },
});

Deno.test({
  name: "estimateSize - ArrayBuffer",
  fn() {
    assertEquals(estimateSize(new Uint8Array([1, 2, 3]).buffer), 12);
  },
});

Deno.test({
  name: "estimateSize - Array",
  fn() {
    assertEquals(estimateSize([1, 2, 3, "boo", true, false, /abc/]), 27);
  },
});

Deno.test({
  name: "estimateSize - Set",
  fn() {
    assertEquals(estimateSize(new Set([1, 2, 3, 4, "foo"])), 18);
  },
});

Deno.test({
  name: "estimateSize - Map",
  fn() {
    assertEquals(
      estimateSize(
        new Map<string, string | number>([["a", 1], ["b", 2], ["c", "d"]]),
      ),
      21,
    );
  },
});

Deno.test({
  name: "estimateSize - object",
  fn() {
    assertEquals(
      estimateSize({ a: new Map([[{ a: 1 }, { b: /234/ }]]), b: false }),
      36,
    );
  },
});

Deno.test({
  name: "estimateSize - Deno.KvU64",
  fn() {
    assertEquals(estimateSize(new Deno.KvU64(100n)), 12);
  },
});

Deno.test({
  name: "estimateSize - object with circular reference",
  fn() {
    // deno-lint-ignore no-explicit-any
    const a = { b: 1 as any };
    const b = { a };
    a.b = b;
    assertEquals(estimateSize(a), 11);
  },
});

Deno.test({
  name: "estimateSize - symbol",
  fn() {
    assertEquals(estimateSize(Symbol.for("@deno/kv-utils")), 0);
  },
});
