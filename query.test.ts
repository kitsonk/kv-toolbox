import { assert } from "@std/assert/assert";
import { assertEquals } from "@std/assert/equals";
import { assertThrows } from "@std/assert/throws";
import { Filter, PropertyPath, query } from "./query.ts";

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
