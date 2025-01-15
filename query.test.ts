import { assert } from "@std/assert/assert";
import { assertEquals } from "@std/assert/equals";
import { assertThrows } from "@std/assert/throws";

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
