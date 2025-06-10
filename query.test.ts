import { assert } from "@std/assert/assert";
import { assertEquals } from "@std/assert/equals";
import { assertThrows } from "@std/assert/throws";
import { timingSafeEqual } from "@std/crypto/timing-safe-equal";

import { set } from "./blob.ts";
import { Filter, PropertyPath, Query, query } from "./query.ts";

Deno.test("PropertyPath - exists", () => {
  const path = new PropertyPath("a", "b", "c");
  assert(path.exists({ a: { b: { c: 1 } } }));
  assert(!path.exists({ a: { b: { d: 1 } } }));
});

Deno.test("PropertyPath - exists - Map", () => {
  const path = new PropertyPath("a", "b", "c");
  assert(path.exists(new Map([["a", new Map([["b", new Map([["c", 1]])]])]])));
  assert(!path.exists(new Map([["a", new Map([["b", new Map([["d", 1]])]])]])));
});

Deno.test("PropertyPath - value", () => {
  const path = new PropertyPath("a", "b", "c");
  assertEquals(path.value({ a: { b: { c: 1 } } }), 1);
});

Deno.test("PropertyPath - value - Map", () => {
  const path = new PropertyPath("a", "b", "c");
  assertEquals(
    path.value(new Map([["a", new Map([["b", new Map([["c", 1]])]])]])),
    1,
  );
});

Deno.test("PropertyPath - value - not exists", () => {
  const path = new PropertyPath("a", "b", "c");
  assertThrows(
    () => path.value({ a: { b: { d: 1 } } }),
    Error,
    "Property does not exist",
  );
});

Deno.test("PropertyPath - value - not mappable", () => {
  const path = new PropertyPath("a", "b", "c");
  assertThrows(
    () => path.value({ a: { b: 1 } }),
    Error,
    "Value is not mappable",
  );
});

Deno.test("Filter.where() - less than", () => {
  const filter = Filter.where("age", "<", 10);
  assert(filter.test({ age: 9 }));
  assert(!filter.test({ age: 10 }));
});

Deno.test("Filter.where() - less than or equal", () => {
  const filter = Filter.where("age", "<=", 10);
  assert(filter.test({ age: 10 }));
  assert(!filter.test({ age: 11 }));
});

Deno.test("Filter.where() - equals", () => {
  const filter = Filter.where("name", "==", "test");
  assert(filter.test({ name: "test" }));
  assert(!filter.test({ name: "test2" }));
});

Deno.test("Filter.where() - equals deeply", () => {
  const filter = Filter.where("name", "==", { a: 1 });
  assert(filter.test({ name: { a: 1 } }));
  assert(!filter.test({ name: { a: 2 } }));
});

Deno.test("Filter.where() - not equals", () => {
  const filter = Filter.where("name", "!=", "test");
  assert(!filter.test({ name: "test" }));
  assert(filter.test({ name: "test2" }));
});

Deno.test("Filter.where() - greater than", () => {
  const filter = Filter.where("age", ">", 10);
  assert(filter.test({ age: 11 }));
  assert(!filter.test({ age: 10 }));
});

Deno.test("Filter.where() - greater than or equal", () => {
  const filter = Filter.where("age", ">=", 10);
  assert(filter.test({ age: 10 }));
  assert(!filter.test({ age: 9 }));
});

Deno.test("Filter.where() - array contains", () => {
  const filter = Filter.where("tags", "array-contains", "test");
  assert(filter.test({ tags: ["test"] }));
  assert(!filter.test({ tags: ["test2"] }));
});

Deno.test("Filter.where() - array contains any", () => {
  const filter = Filter.where("tags", "array-contains-any", ["test", "test2"]);
  assert(filter.test({ tags: ["test"] }));
  assert(filter.test({ tags: ["test2"] }));
  assert(!filter.test({ tags: ["test3"] }));
});

Deno.test("Filter.where() - in", () => {
  const filter = Filter.where("age", "in", [10, 20]);
  assert(filter.test({ age: 10 }));
  assert(filter.test({ age: 20 }));
  assert(!filter.test({ age: 30 }));
});

Deno.test("Filter.where() - not in", () => {
  const filter = Filter.where("age", "not-in", [10, 20]);
  assert(!filter.test({ age: 10 }));
  assert(!filter.test({ age: 20 }));
  assert(filter.test({ age: 30 }));
});

Deno.test("Filter.where() - matches", () => {
  const filter = Filter.where("name", "matches", /^test/);
  assert(filter.test({ name: "test" }));
  assert(filter.test({ name: "test2" }));
  assert(!filter.test({ name: " test" }));
});

Deno.test("Filter.where() - kind of - string", () => {
  const filter = Filter.where("name", "kind-of", "string");
  assert(filter.test({ name: "test" }));
  assert(!filter.test({ name: 12 }));
});

Deno.test("Filter.where() - kind of - number", () => {
  const filter = Filter.where("age", "kind-of", "number");
  assert(filter.test({ age: 12 }));
  assert(!filter.test({ age: "12" }));
});

Deno.test("Filter.where() - kind of - boolean", () => {
  const filter = Filter.where("active", "kind-of", "boolean");
  assert(filter.test({ active: true }));
  assert(!filter.test({ active: "true" }));
});

Deno.test("Filter.where() - kind of - undefined", () => {
  const filter = Filter.where("active", "kind-of", "undefined");
  assert(filter.test({ active: undefined }));
  assert(!filter.test({ active: "undefined" }));
  assert(!filter.test({ active: null }));
  assert(!filter.test({}));
});

Deno.test("Filter.where() - kind of - null", () => {
  const filter = Filter.where("active", "kind-of", "null");
  assert(filter.test({ active: null }));
  assert(!filter.test({ active: "null" }));
  assert(!filter.test({ active: undefined }));
  assert(!filter.test({}));
});

Deno.test("Filter.where() - kind of - bigint", () => {
  const filter = Filter.where("age", "kind-of", "bigint");
  assert(filter.test({ age: 12n }));
  assert(!filter.test({ age: 12 }));
});

Deno.test("Filter.where() - kind of - Date", () => {
  const filter = Filter.where("createdAt", "kind-of", "Date");
  assert(filter.test({ createdAt: new Date() }));
  assert(!filter.test({ createdAt: Date.now() }));
});

Deno.test("Filter.where() - kind of - Map", () => {
  const filter = Filter.where("tags", "kind-of", "Map");
  assert(filter.test({ tags: new Map() }));
  assert(!filter.test({ tags: {} }));
});

Deno.test("Filter.where() - kind of - Set", () => {
  const filter = Filter.where("tags", "kind-of", "Set");
  assert(filter.test({ tags: new Set() }));
  assert(!filter.test({ tags: [] }));
});

Deno.test("Filter.where() - kind of - RegExp", () => {
  const filter = Filter.where("tags", "kind-of", "RegExp");
  assert(filter.test({ tags: /^test/ }));
  assert(!filter.test({ tags: {} }));
});

Deno.test("Filter.where() - kind of - Array", () => {
  const filter = Filter.where("tags", "kind-of", "Array");
  assert(filter.test({ tags: ["1"] }));
  assert(!filter.test({ tags: { 1: true } }));
});

Deno.test("Filter.where() - kind of - KvU64", () => {
  const filter = Filter.where("tags", "kind-of", "KvU64");
  assert(filter.test({ tags: new Deno.KvU64(1n) }));
  assert(!filter.test({ tags: 1n }));
});

Deno.test("Filter.where() - kind of - ArrayBuffer", () => {
  const filter = Filter.where("data", "kind-of", "ArrayBuffer");
  assert(filter.test({ data: new ArrayBuffer(0) }));
  assert(!filter.test({ data: [] }));
});

Deno.test("Filter.where() - kind of - DataView", () => {
  const filter = Filter.where("data", "kind-of", "DataView");
  assert(filter.test({ data: new DataView(new ArrayBuffer(0)) }));
  assert(!filter.test({ data: [] }));
});

Deno.test("Filter.where() - kind of - Int8Array", () => {
  const filter = Filter.where("data", "kind-of", "Int8Array");
  assert(filter.test({ data: new Int8Array([1, 2, 3]) }));
  assert(!filter.test({ data: new Uint8Array([1, 2, 3]) }));
  assert(!filter.test({ data: [1, 2, 3] }));
});

Deno.test("Filter.where() - kind of - TypeError", () => {
  const filter = Filter.where("error", "kind-of", "TypeError");
  assert(filter.test({ error: new TypeError("ooops!") }));
  assert(!filter.test({ error: new Error("ooops!") }));
  assert(!filter.test({ error: "TypeError" }));
  assert(!filter.test({ error: { message: "ooops!" } }));
});

Deno.test("Filter.where() - kind of - object", () => {
  const filter = Filter.where("tags", "kind-of", "object");
  assert(filter.test({ tags: {} }));
  assert(!filter.test({ tags: [] }));
});

Deno.test("Filter.where() - PropertyPath", () => {
  const filter = Filter.where(new PropertyPath("a", "b", "c"), "==", 1);
  assert(filter.test({ a: { b: { c: 1 } } }));
  assert(!filter.test({ a: { b: { c: 2 } } }));
  assert(!filter.test({ a: { b: { d: 1 } } }));
});

Deno.test("Filter.and()", () => {
  const filter = Filter.and(
    Filter.where("age", ">", 10),
    Filter.where("age", "<", 20),
  );
  assert(filter.test({ age: 15 }));
  assert(!filter.test({ age: 10 }));
  assert(!filter.test({ age: 20 }));
});

Deno.test("Filter.or()", () => {
  const filter = Filter.or(
    Filter.where("age", "<", 10),
    Filter.where("age", ">", 20),
  );
  assert(filter.test({ age: 5 }));
  assert(filter.test({ age: 25 }));
  assert(!filter.test({ age: 15 }));
});

Deno.test("Filter.where() - toJSON", () => {
  const filter = Filter.where("name", "matches", /^test/);
  assertEquals(filter.toJSON(), {
    kind: "where",
    property: "name",
    operation: "matches",
    value: { type: "RegExp", value: "/^test/" },
  });
});

Deno.test("Filter.value() - toJSON", () => {
  const filter = Filter.value("matches", /^test/);
  assertEquals(filter.toJSON(), {
    kind: "value",
    operation: "matches",
    value: { type: "RegExp", value: "/^test/" },
  });
});

Deno.test("Filter.or() - toJSON", () => {
  const filter = Filter.or(
    Filter.value("matches", /^test/),
    Filter.where("age", ">", 10),
  );
  assertEquals(filter.toJSON(), {
    kind: "or",
    filters: [
      {
        kind: "value",
        operation: "matches",
        value: { type: "RegExp", value: "/^test/" },
      },
      {
        kind: "where",
        property: "age",
        operation: ">",
        value: { type: "number", value: 10 },
      },
    ],
  });
});

Deno.test("Filter.and() - toJSON", () => {
  const filter = Filter.and(
    Filter.value("matches", /^test/),
    Filter.where("age", ">", 10),
  );
  assertEquals(filter.toJSON(), {
    kind: "and",
    filters: [
      {
        kind: "value",
        operation: "matches",
        value: { type: "RegExp", value: "/^test/" },
      },
      {
        kind: "where",
        property: "age",
        operation: ">",
        value: { type: "number", value: 10 },
      },
    ],
  });
});

Deno.test("query() - value", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a"], { age: 10 })
    .set(["b"], { age: 20 })
    .commit();
  const result = query(db, { prefix: [] }).value("==", { age: 10 }).get();
  const entries = [];
  for await (const entry of result) {
    entries.push(entry);
  }
  assertEquals(entries.length, 1);
  assertEquals(entries[0].key, ["a"]);
  assertEquals(entries[0].value, { age: 10 });
  db.close();
});

Deno.test("query() - where - equals", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a"], { age: 10 })
    .set(["b"], { age: 20 })
    .commit();
  const result = query(db, { prefix: [] }).where("age", "==", 10).get();
  const entries = [];
  for await (const entry of result) {
    entries.push(entry);
  }
  assertEquals(entries.length, 1);
  assertEquals(entries[0].key, ["a"]);
  assertEquals(entries[0].value, { age: 10 });
  db.close();
});

Deno.test("query() - keys", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a"], { age: 10 })
    .set(["b"], { age: 20 })
    .set(["c"], { age: 30 })
    .commit();
  const result = await query(db, { prefix: [] })
    .where("age", ">", 10)
    .keys();
  assertEquals(result, [["b"], ["c"]]);
  db.close();
});

Deno.test("query() - unique", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a", "b"], { age: 10 })
    .set(["a", "b", "c"], { age: 10 })
    .set(["a", "d", "e"], { age: 10 })
    .set(["a", "d", "f"], { age: 10 })
    .set(["a", "g"], { age: 20 })
    .commit();
  const result = await query(db, { prefix: ["a"] })
    .where("age", "==", 10)
    .unique();
  assertEquals(result, [["a", "b"], ["a", "d"]]);
  db.close();
});

Deno.test("query() - counts", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a", "b"], { age: 10 })
    .set(["a", "b", "c"], { age: 10 })
    .set(["a", "d", "e"], { age: 10 })
    .set(["a", "d", "f"], { age: 10 })
    .set(["a", "g"], { age: 20 })
    .commit();
  const result = await query(db, { prefix: ["a"] })
    .where("age", "==", 10)
    .counts();
  assertEquals(result, [
    { key: ["a", "b"], count: 1 },
    { key: ["a", "d"], count: 2 },
  ]);
  db.close();
});

Deno.test("query() - tree", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a", "b"], { age: 10 })
    .set(["a", "b", "c"], { age: 10 })
    .set(["a", "d", "e"], { age: 10 })
    .set(["a", "d", "f"], { age: 10 })
    .set(["a", "g"], { age: 20 })
    .set(["b", "h"], { age: 10 })
    .commit();
  const result = await query(db, { prefix: [] })
    .where("age", "==", 10)
    .tree();
  assertEquals(result, {
    children: [
      {
        part: "a",
        children: [
          {
            part: "b",
            hasValue: true,
            children: [{ part: "c", hasValue: true }],
          },
          {
            part: "d",
            children: [
              { part: "e", hasValue: true },
              { part: "f", hasValue: true },
            ],
          },
        ],
      },
      { part: "b", children: [{ part: "h", hasValue: true }] },
    ],
  });
  db.close();
});

Deno.test("Query - toJSON", async () => {
  const db = await Deno.openKv(":memory:");
  const result = query(db, { prefix: ["a"] })
    .where("age", "==", 10)
    .toJSON();
  assertEquals(result, {
    selector: { prefix: [{ type: "string", value: "a" }] },
    options: {},
    filters: [{
      kind: "where",
      property: "age",
      operation: "==",
      value: { type: "number", value: 10 },
    }],
  });
  db.close();
});

Deno.test("Query.parse()", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a", "b"], { age: 10 })
    .set(["a", "b", "c"], { age: 10 })
    .set(["a", "d", "e"], { age: 10 })
    .set(["a", "d", "f"], { age: 10 })
    .set(["a", "g"], { age: 20 })
    .set(["b", "h"], { age: 10 })
    .commit();
  const q = Query.parse(db, {
    selector: { prefix: [{ type: "string", value: "a" }] },
    options: {},
    filters: [{
      kind: "where",
      property: "age",
      operation: "==",
      value: { type: "number", value: 10 },
    }],
  });
  const result = await q.keys();
  assertEquals(result, [
    ["a", "b"],
    ["a", "b", "c"],
    ["a", "d", "e"],
    ["a", "d", "f"],
  ]);
  db.close();
});

Deno.test("Query - limit on construction", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a", "b"], { age: 10 })
    .set(["a", "b", "c"], { age: 10 })
    .set(["a", "d", "e"], { age: 10 })
    .set(["a", "d", "f"], { age: 10 })
    .set(["a", "g"], { age: 20 })
    .set(["b", "h"], { age: 10 })
    .commit();
  const result = await query(db, { prefix: [] }, { limit: 2 })
    .where("age", "==", 10)
    .keys();
  assertEquals(result, [["a", "b"], ["a", "b", "c"]]);
  db.close();
});

Deno.test("Query - limit API", async () => {
  const db = await Deno.openKv(":memory:");
  await db
    .atomic()
    .set(["a", "b"], { age: 10 })
    .set(["a", "b", "c"], { age: 10 })
    .set(["a", "d", "e"], { age: 10 })
    .set(["a", "d", "f"], { age: 10 })
    .set(["a", "g"], { age: 20 })
    .set(["b", "h"], { age: 10 })
    .commit();
  const result = await query(db, { prefix: [] })
    .where("age", "==", 10)
    .limit(2)
    .keys();
  assertEquals(result, [["a", "b"], ["a", "b", "c"]]);
  db.close();
});

Deno.test({
  name: "query - blob with limit",
  async fn() {
    const kv = await Deno.openKv(":memory:");
    await set(kv, ["hello", 1], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 2], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 3], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello"], new Uint8Array([1, 2, 3]));
    await set(kv, ["world"], new Uint8Array([1, 2, 3]));
    const entries = await Array.fromAsync(query(kv, { prefix: ["hello"] }, { limit: 2, meta: true }).get());
    assertEquals(entries.length, 2);
    for (const [idx, entry] of entries.entries()) {
      assertEquals(entry.key, ["hello", idx + 1]);
      assertEquals(entry.value, { kind: "buffer", size: 3 });
    }
    kv.close();
  },
});

Deno.test({
  name: "query - blob as meta",
  async fn() {
    const kv = await Deno.openKv(":memory:");
    await set(kv, ["hello", 1], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 2], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 3], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello"], new Uint8Array([1, 2, 3]));
    await set(kv, ["world"], new Uint8Array([1, 2, 3]));
    const entries = await Array.fromAsync(query(kv, { prefix: ["hello"] }, { meta: true }).get());
    assertEquals(entries.length, 3);
    for (const [idx, entry] of entries.entries()) {
      assertEquals(entry.key, ["hello", idx + 1]);
      assertEquals(entry.value, { kind: "buffer", size: 3 });
    }
    kv.close();
  },
});

Deno.test({
  name: "query - blob as bytes",
  async fn() {
    const kv = await Deno.openKv(":memory:");
    await set(kv, ["hello", 1], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 2], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 3], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello"], new Uint8Array([1, 2, 3]));
    await set(kv, ["world"], new Uint8Array([1, 2, 3]));
    const entries = await Array.fromAsync(query(kv, { prefix: ["hello"] }, { bytes: true }).get());
    assertEquals(entries.length, 3);
    for (const [idx, entry] of entries.entries()) {
      assertEquals(entry.key, ["hello", idx + 1]);
      assert(timingSafeEqual(entry.value, new Uint8Array([1, 2, 3])));
    }
    kv.close();
  },
});

Deno.test({
  name: "query - blob as blob",
  async fn() {
    const kv = await Deno.openKv(":memory:");
    await set(kv, ["hello", 1], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 2], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 3], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello"], new Uint8Array([1, 2, 3]));
    await set(kv, ["world"], new Uint8Array([1, 2, 3]));
    const entries = await Array.fromAsync(query(kv, { prefix: ["hello"] }, { blob: true }).get());
    assertEquals(entries.length, 3);
    for (const [idx, entry] of entries.entries()) {
      assertEquals(entry.key, ["hello", idx + 1]);
      assert(entry.value instanceof Blob);
      assert(
        timingSafeEqual(await entry.value.bytes(), new Uint8Array([1, 2, 3])),
      );
    }
    kv.close();
  },
});

Deno.test({
  name: "query - blob as stream",
  async fn() {
    const kv = await Deno.openKv(":memory:");
    await set(kv, ["hello", 1], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 2], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello", 3], new Uint8Array([1, 2, 3]));
    await set(kv, ["hello"], new Uint8Array([1, 2, 3]));
    await set(kv, ["world"], new Uint8Array([1, 2, 3]));
    const entries = await Array.fromAsync(query(kv, { prefix: ["hello"] }, { stream: true }).get());
    assertEquals(entries.length, 3);
    for (const [idx, entry] of entries.entries()) {
      assertEquals(entry.key, ["hello", idx + 1]);
      assert(entry.value instanceof ReadableStream);
    }
    kv.close();
  },
});
