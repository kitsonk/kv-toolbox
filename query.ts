/**
 * Utilities for querying/filtering entries from a {@linkcode Deno.Kv} instance.
 *
 * @module
 */

import { equal } from "@std/assert/equal";
import { assert } from "@std/assert/assert";

/**
 * The supported operations for filtering entries.
 *
 * The following operations are supported:
 *
 * - `"<"` - less than
 * - `"<="` - less than or equal
 * - `"=="` - equal
 * - `">="` - greater than or equal
 * - `">"` - greater than
 * - `"!="` - not equal
 * - `"array-contains"` - array contains the value
 * - `"array-contains-any"` - array contains any of the values
 * - `"in"` - value is in the array of supplied values
 * - `"not-in"` - value is not in the array of supplied values
 * - `"matches"` - value matches the regular expression
 */
export type Operation =
  | "<"
  | "<="
  | "=="
  | ">="
  | ">"
  | "!="
  | "array-contains"
  | "array-contains-any"
  | "in"
  | "not-in"
  | "matches";

type Mappable = Record<string, unknown> | Map<string, unknown>;

function getValue(obj: Mappable, key: string): unknown {
  if (obj instanceof Map) {
    return obj.get(key);
  }
  return obj[key];
}

function hasProperty(obj: Mappable, key: string): boolean {
  if (obj instanceof Map) {
    return obj.has(key);
  }
  return key in obj;
}

function isMappable(value: unknown): value is Mappable {
  return typeof value === "object" && value !== null;
}

/**
 * A representation of a property path to a value in an object. This is used to
 * be able to query/filter entries based on nested properties.
 *
 * @example
 *
 * ```ts
 * import { PropertyPath } from "@kitsonk/kv-toolbox/query";
 * import { assert } from "@std/assert/assert";
 *
 * const path = new PropertyPath("a", "b", "c");
 * assert(path.exists({ a: { b: { c: 1 } } }));
 * assert(!path.exists({ a: { b: { d: 1 } } }));
 * assert(path.value({ a: { b: { c: 1 } } }) === 1);
 * ```
 */
export class PropertyPath {
  #parts: string[];

  constructor(...parts: string[]) {
    this.#parts = parts;
  }

  /**
   * Returns `true` if the property path exists in the object.
   */
  exists(other: unknown): other is Mappable {
    let current = other;
    for (const part of this.#parts) {
      if (!isMappable(current)) {
        return false;
      }
      if (!hasProperty(current, part)) {
        return false;
      }
      current = getValue(current, part);
    }
    return true;
  }

  /**
   * Returns the value of the property represented by the path. If the property
   * does not exist, an error is thrown.
   */
  value(other: Mappable): unknown {
    // deno-lint-ignore no-explicit-any
    let current: any = other;
    for (const part of this.#parts) {
      if (!isMappable(current)) {
        throw new TypeError("Value is not mappable");
      }
      if (!hasProperty(current, part)) {
        throw new Error("Property does not exist");
      }
      current = getValue(current, part);
    }
    return current;
  }
}

// deno-lint-ignore no-explicit-any
function exec(other: any, operation: Operation, value: any | any[]): boolean {
  switch (operation) {
    case "<":
      return other < value;
    case "<=":
      return other <= value;
    case "!=":
      return !equal(other, value);
    case "==":
      return equal(other, value);
    case ">":
      return other > value;
    case ">=":
      return other >= value;
    case "array-contains":
      if (Array.isArray(other)) {
        return other.some((v) => equal(v, value));
      }
      return false;
    case "array-contains-any":
      if (Array.isArray(other) && Array.isArray(value)) {
        return other.some((v) => value.some((w) => equal(v, w)));
      }
      return false;
    case "in":
      if (Array.isArray(value)) {
        return value.some((v) => equal(other, v));
      }
      return false;
    case "not-in":
      if (Array.isArray(value)) {
        return !value.some((v) => equal(other, v));
      }
      break;
    case "matches":
      if (typeof other === "string" && value instanceof RegExp) {
        return value.test(other);
      }
      break;
  }
  return false;
}

/**
 * The interface which is used by {@linkcode query} to filter entries. As long
 * as the object implements the `.test()` method, it can be used as a filter.
 * It will be passed the value of the entry and should return `true` if the
 * entry should be included in the results.
 */
export interface FilterLike {
  test(value: unknown): boolean;
}

function isFilterLike(value: unknown): value is FilterLike {
  return typeof value === "object" && value !== null && "test" in value &&
    typeof value.test === "function";
}

/**
 * A filter instance which can be used to filter entries based on a set of
 * conditions. Users should use the static methods to create instances of this
 * class.
 *
 * @example Creating a filter based on a property value
 *
 * ```ts
 * import { Filter } from "@kitsonk/kv-toolbox/query";
 * import { assert } from "@std/assert/assert";
 *
 * const filter = Filter.where("age", "<=", 10);
 * assert(filter.test({ age: 10 }));
 * assert(!filter.test({ age: 11 }));
 * ```
 *
 * @example Creating a filter based on a property value using a `PropertyPath`
 *
 * ```ts
 * import { Filter, PropertyPath } from "@kitsonk/kv-toolbox/query";
 * import { assert } from "@std/assert/assert";
 *
 * const filter = Filter.where(new PropertyPath("a", "b", "c"), "==", 1);
 * assert(filter.test({ a: { b: { c: 1 } } }));
 * assert(!filter.test({ a: { b: { c: 2 } } }));
 * assert(!filter.test({ a: { b: { d: 1 } } }));
 * ```
 *
 * @example Creating a filter based on an _or_ condition
 *
 * ```ts
 * import { Filter } from "@kitsonk/kv-toolbox/query";
 * import { assert } from "@std/assert/assert";
 *
 * const filter = Filter.or(
 *   Filter.where("age", "<", 10),
 *   Filter.where("age", ">", 20),
 * );
 * assert(filter.test({ age: 5 }));
 * assert(filter.test({ age: 25 }));
 * assert(!filter.test({ age: 15 }));
 * ```
 *
 * @example Creating a filter based on an _and_ condition
 *
 * ```ts
 * import { Filter } from "@kitsonk/kv-toolbox/query";
 * import { assert } from "@std/assert/assert";
 *
 * const filter = Filter.and(
 *  Filter.where("age", ">", 10),
 *  Filter.where("age", "<", 20),
 * );
 * assert(filter.test({ age: 15 }));
 * assert(!filter.test({ age: 10 }));
 * assert(!filter.test({ age: 20 }));
 * ```
 */
export class Filter implements FilterLike {
  #condition: boolean | ((value: unknown) => boolean);
  #filters: Filter[];

  private constructor(
    condition: boolean | ((value: unknown) => boolean),
    filters: Filter[] = [],
  ) {
    this.#condition = condition;
    this.#filters = filters;
  }

  /**
   * Test the value against the filter.
   */
  test(value: unknown): boolean {
    if (typeof this.#condition === "boolean") {
      return this.#condition
        ? this.#filters.every((f) => f.test(value))
        : this.#filters.some((f) => f.test(value));
    }
    return this.#condition(value);
  }

  /**
   * Create a filter which will return `true` if all the filters return `true`.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.and(
   *   Filter.where("age", ">", 10),
   *   Filter.where("age", "<", 20),
   * );
   * assert(filter.test({ age: 15 }));
   * assert(!filter.test({ age: 10 }));
   * assert(!filter.test({ age: 20 }));
   * ```
   */
  static and(...filters: Filter[]): Filter {
    return new Filter(true, filters);
  }

  /**
   * Create a filter which will return `true` if any of the filters return
   * `true`.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.or(
   *   Filter.where("age", "<", 10),
   *   Filter.where("age", ">", 20),
   * );
   * assert(filter.test({ age: 5 }));
   * assert(filter.test({ age: 25 }));
   * assert(!filter.test({ age: 15 }));
   * ```
   */
  static or(...filters: Filter[]): Filter {
    return new Filter(false, filters);
  }

  /**
   * Create a filter which will return `true` if the value matches the
   * operation and value.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.value("==", 10);
   * assert(filter.test(10));
   * assert(!filter.test(11));
   * ```
   */
  static value(operation: "matches", value: RegExp): Filter;
  /**
   * Create a filter which will return `true` if the value matches the
   * operation and value.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.value("==", 10);
   * assert(filter.test(10));
   * assert(!filter.test(11));
   * ```
   */
  static value(
    operation: "in" | "not-in" | "array-contains-any",
    value: unknown[],
  ): Filter;
  /**
   * Create a filter which will return `true` if the value matches the
   * operation and value.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.value("==", 10);
   * assert(filter.test(10));
   * assert(!filter.test(11));
   * ```
   */
  static value(operation: Operation, value: unknown): Filter;
  static value(operation: Operation, value: unknown | unknown[]): Filter {
    return new Filter((other) => exec(other, operation, value));
  }

  /**
   * Create a filter which will return `true` if the value of the property
   * matches the operation and value.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.where("age", "<=", 10);
   * assert(filter.test({ age: 10 }));
   * assert(!filter.test({ age: 11 }));
   * ```
   */
  static where(
    property: string | PropertyPath,
    operation: "matches",
    value: RegExp,
  ): Filter;
  /**
   * Create a filter which will return `true` if the value of the property
   * matches the operation and value.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.where("age", "<=", 10);
   * assert(filter.test({ age: 10 }));
   * assert(!filter.test({ age: 11 }));
   * ```
   */
  static where(
    property: string | PropertyPath,
    operation: "in" | "not-in" | "array-contains-any",
    value: unknown[],
  ): Filter;
  /**
   * Create a filter which will return `true` if the value of the property
   * matches the operation and value.
   *
   * @example
   *
   * ```ts
   * import { Filter } from "@kitsonk/kv-toolbox/query";
   * import { assert } from "@std/assert/assert";
   *
   * const filter = Filter.where("age", "<=", 10);
   * assert(filter.test({ age: 10 }));
   * assert(!filter.test({ age: 11 }));
   * ```
   */
  static where(
    property: string | PropertyPath,
    operation: Operation,
    value: unknown,
  ): Filter;
  static where(
    property: string | PropertyPath,
    operation: Operation,
    value: unknown | unknown[],
  ): Filter {
    return new Filter((other) => {
      if (property instanceof PropertyPath) {
        if (property.exists(other)) {
          const propValue = property.value(other);
          return exec(propValue, operation, value);
        }
        return false;
      }
      if (isMappable(other)) {
        if (hasProperty(other, property)) {
          const propValue = getValue(other, property);
          return exec(propValue, operation, value);
        }
        return false;
      }
      return false;
    });
  }
}

const AsyncIterator = Object.getPrototypeOf(async function* () {}).constructor;

class QueryListIterator<T = unknown> extends AsyncIterator
  implements Deno.KvListIterator<T> {
  #iterator: Deno.KvListIterator<T>;
  #query: FilterLike[];

  get cursor(): string {
    return this.#iterator.cursor;
  }

  constructor(iterator: Deno.KvListIterator<T>, query: FilterLike[]) {
    super();
    this.#iterator = iterator;
    this.#query = query;
  }

  async next(): Promise<IteratorResult<Deno.KvEntry<T>, undefined>> {
    for await (const entry of this.#iterator) {
      if (this.#query.every((f) => f.test(entry.value))) {
        return { value: entry, done: false };
      }
    }
    return { value: undefined, done: true };
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Deno.KvEntry<T>> {
    return this;
  }
}

/**
 * Query instance for filtering entries from a {@linkcode Deno.Kv} instance.
 */
export class Query<T = unknown> {
  #kv: Deno.Kv;
  #selector: Deno.KvListSelector;
  #options: Deno.KvListOptions;
  #query: FilterLike[] = [];

  constructor(
    kv: Deno.Kv,
    selector: Deno.KvListSelector,
    options: Deno.KvListOptions = {},
  ) {
    this.#kv = kv;
    this.#selector = selector;
    this.#options = options;
  }

  /**
   * Get the entries that match the query conditions.
   */
  get(): Deno.KvListIterator<T> {
    return new QueryListIterator<T>(
      this.#kv.list<T>(this.#selector, this.#options),
      this.#query,
    );
  }

  /**
   * Add a filter to the query where the value of the entry matches the
   * operation and value.
   *
   * @example
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const db = await Deno.openKv();
   * const result = query(db, { prefix: [] })
   *   .value("==", { age: 10 })
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * db.close();
   * ```
   */
  value(operation: "matches", value: RegExp): this;
  /**
   * Add a filter to the query where the value of the entry matches the
   * operation and value.
   *
   * @example
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const db = await Deno.openKv();
   * const result = query(db, { prefix: [] })
   *   .value("==", { age: 10 })
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * db.close();
   * ```
   */
  value(
    operation: "in" | "not-in" | "array-contains-any",
    value: unknown[],
  ): this;
  /**
   * Add a filter to the query where the value of the entry matches the
   * operation and value.
   *
   * @example
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const db = await Deno.openKv();
   * const result = query(db, { prefix: [] })
   *   .value("==", { age: 10 })
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * db.close();
   * ```
   */
  value(operation: Operation, value: unknown): this;
  value(operation: Operation, value: unknown | unknown[]): this {
    this.#query.push(Filter.value(operation, value));
    return this;
  }

  /**
   * Add a filter to the query. Only entries which values match all the filters
   * will be returned.
   *
   * @example
   *
   * ```ts
   * import { query, Filter } from "@kitsonk/kv-toolbox/query";
   *
   * const db = await Deno.openKv();
   * const result = query(db, { prefix: [] })
   *   .where(Filter.and(
   *      Filter.where("age", ">", 10),
   *      Filter.where("age", "<", 20),
   *   ))
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * db.close();
   * ```
   */
  where(filter: FilterLike): this;
  /**
   * Add a property filter to the query. Only entries which values match the
   * filter will be returned.
   *
   * @example
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const db = await Deno.openKv();
   * const result = query(db, { prefix: [] })
   *   .where("age", "<=", 10)
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * db.close();
   * ```
   */
  where(
    property: string | PropertyPath,
    operation: "matches",
    value: RegExp,
  ): this;
  /**
   * Add a property filter to the query. Only entries which values match the
   * filter will be returned.
   *
   * @example
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const db = await Deno.openKv();
   * const result = query(db, { prefix: [] })
   *   .where("age", "<=", 10)
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * db.close();
   * ```
   */
  where(
    property: string | PropertyPath,
    operation: "in" | "not-in" | "array-contains-any",
    value: unknown[],
  ): this;
  /**
   * Add a property filter to the query. Only entries which values match the
   * filter will be returned.
   *
   * @example
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const db = await Deno.openKv();
   * const result = query(db, { prefix: [] })
   *   .where("age", "<=", 10)
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * db.close();
   * ```
   */
  where(
    property: string | PropertyPath,
    operation: Operation,
    value: unknown,
  ): this;
  where(
    propertyOrFilter: FilterLike | string | PropertyPath,
    operation?: Operation,
    value?: unknown | unknown[],
  ): this {
    if (isFilterLike(propertyOrFilter)) {
      this.#query.push(propertyOrFilter);
    } else {
      assert(operation, "Operation is required");
      this.#query.push(Filter.where(propertyOrFilter, operation, value));
    }
    return this;
  }
}

/**
 * Query/filter entries from a {@linkcode Deno.Kv} instance.
 *
 * The query instance can be used to filter entries based on a set of
 * conditions. Then the filtered entries can be retrieved using the `.get()`
 * method, which returns an async iterator that will yield the entries that
 * match the conditions.
 *
 * At a base level a query works like the `Deno.Kv.prototype.list()` method, but
 * with the added ability to filter entries based on the query conditions.
 *
 * @example Filtering entries based on a property value
 *
 * ```ts
 * import { query } from "@kitsonk/kv-toolbox/query";
 * const db = await Deno.openKv();
 * const result = query(db, { prefix: [] })
 *   .where("age", "<=", 10)
 *   .get();
 * for await (const entry of result) {
 *   console.log(entry);
 * }
 * db.close();
 * ```
 *
 * @example Filtering entries based on a property value using a `PropertyPath`
 *
 * ```ts
 * import { query, PropertyPath } from "@kitsonk/kv-toolbox/query";
 *
 * const db = await Deno.openKv();
 * const result = query(db, { prefix: [] })
 *   // matches { a: { b: { c: 1 } } }
 *   .where(new PropertyPath("a", "b", "c"), "==", 1)
 *   .get();
 * for await (const entry of result) {
 *   console.log(entry);
 * }
 * db.close();
 * ```
 *
 * @example Filtering entries based on an _or_ condition
 *
 * ```ts
 * import { query, Filter } from "@kitsonk/kv-toolbox/query";
 *
 * const db = await Deno.openKv();
 * const result = query(db, { prefix: [] })
 *   .where(Filter.or(
 *     Filter.where("age", "<", 10),
 *     Filter.where("age", ">", 20),
 *   ))
 *   .get();
 * for await (const entry of result) {
 *   console.log(entry);
 * }
 * db.close();
 * ```
 *
 * @template T the type of the value stored in the {@linkcode Deno.Kv} instance
 * @param kv the target {@linkcode Deno.Kv} instance
 * @param selector the selector to use for selecting entries
 * @param options
 * @returns
 */
export function query<T = unknown>(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options?: Deno.KvListOptions,
): Query<T> {
  return new Query(kv, selector, options);
}
