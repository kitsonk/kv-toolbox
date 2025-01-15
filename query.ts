/**
 * Utilities for querying/filtering entries from a {@linkcode Deno.Kv} instance.
 *
 * @module
 */

import {
  keyToJSON,
  type KvKeyJSON,
  type KvValueJSON,
  toKey,
  toValue,
  valueToJSON,
} from "@deno/kv-utils/json";
import { equal } from "@std/assert/equal";
import { assert } from "@std/assert/assert";

import {
  keys,
  type KeyTree,
  tree,
  unique,
  uniqueCount,
  type UniqueCountElement,
} from "./keys.ts";

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

export interface QueryLike<T = unknown> {
  readonly selector: Deno.KvListSelector;
  get(): Deno.KvListIterator<T>;
}

/**
 * A representation of a {@linkcode Deno.KvListSelector} as a JSON object.
 */
export type KvListSelectorJSON =
  | { prefix: KvKeyJSON }
  | { prefix: KvKeyJSON; start: KvKeyJSON }
  | { prefix: KvKeyJSON; end: KvKeyJSON }
  | { start: KvKeyJSON; end: KvKeyJSON };

/**
 * A representation of an _and_ filter as a JSON object.
 */
export interface KvFilterAndJSON {
  kind: "and";
  filters: KvFilterJSON[];
}

/**
 * A representation of an _or_ filter as a JSON object.
 */
export interface KvFilterOrJSON {
  kind: "or";
  filters: KvFilterJSON[];
}

/**
 * A representation of a _where_ filter as a JSON object.
 */
export interface KvFilterWhereJSON {
  kind: "where";
  property: string | string[];
  operation: Operation;
  value: KvValueJSON;
}

/**
 * A representation of a _value_ filter as a JSON object.
 */
export interface KvFilterValueJSON {
  kind: "value";
  operation: Operation;
  value: KvValueJSON;
}

/**
 * A representation of a filter as a JSON object.
 */
export type KvFilterJSON =
  | KvFilterAndJSON
  | KvFilterOrJSON
  | KvFilterWhereJSON
  | KvFilterValueJSON;

/**
 * A representation of a query as a JSON object.
 */
export interface KvQueryJSON {
  selector: KvListSelectorJSON;
  options?: Deno.KvListOptions;
  filters: KvFilterJSON[];
}

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

function selectorToJSON(selector: Deno.KvListSelector): KvListSelectorJSON {
  if ("prefix" in selector) {
    if ("start" in selector) {
      return {
        prefix: keyToJSON(selector.prefix),
        start: keyToJSON(selector.start),
      };
    }
    if ("end" in selector) {
      return {
        prefix: keyToJSON(selector.prefix),
        end: keyToJSON(selector.end),
      };
    }
    return { prefix: keyToJSON(selector.prefix) };
  }
  return { start: keyToJSON(selector.start), end: keyToJSON(selector.end) };
}

function toSelector(json: KvListSelectorJSON): Deno.KvListSelector {
  if ("prefix" in json) {
    if ("start" in json) {
      return {
        prefix: toKey(json.prefix),
        start: toKey(json.start),
      };
    }
    if ("end" in json) {
      return {
        prefix: toKey(json.prefix),
        end: toKey(json.end),
      };
    }
    return { prefix: toKey(json.prefix) };
  }
  return { start: toKey(json.start), end: toKey(json.end) };
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

  /**
   * Convert the property path to a JSON array.
   */
  toJSON(): string[] {
    return this.#parts;
  }

  /**
   * Create a property path from an array of parts.
   */
  static from(parts: string[]): PropertyPath {
    return new PropertyPath(...parts);
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
export class Filter {
  #kind: "and" | "or" | "value" | "where";
  #property?: string | PropertyPath;
  #filters: Filter[];
  #operation?: Operation;
  #value?: unknown | unknown[];

  private constructor(kind: "and" | "or", filters: Filter[]);
  private constructor(
    kind: "where",
    filters: undefined,
    property: string | PropertyPath,
    operation: Operation,
    value: unknown | unknown[],
  );
  private constructor(
    kind: "value",
    filters: undefined,
    property: undefined,
    operation: Operation,
    value: unknown | unknown[],
  );
  private constructor(
    kind: "and" | "or" | "value" | "where",
    filters: Filter[] = [],
    property?: string | PropertyPath,
    operation?: Operation,
    value?: unknown | unknown[],
  ) {
    this.#kind = kind;
    this.#filters = filters;
    this.#property = property;
    this.#operation = operation;
    this.#value = value;
  }

  /**
   * Test the value against the filter.
   */
  test(value: unknown): boolean {
    switch (this.#kind) {
      case "and":
        return this.#filters.every((f) => f.test(value));
      case "or":
        return this.#filters.some((f) => f.test(value));
      case "where":
        assert(this.#property);
        assert(this.#operation);
        if (this.#property instanceof PropertyPath) {
          if (this.#property.exists(value)) {
            const propValue = this.#property.value(value);
            return exec(propValue, this.#operation, this.#value);
          }
          return false;
        }
        if (isMappable(value)) {
          if (hasProperty(value, this.#property)) {
            const propValue = getValue(value, this.#property);
            return exec(propValue, this.#operation, this.#value);
          }
          return false;
        }
        return false;
      case "value":
        assert(this.#operation);
        return exec(value, this.#operation, this.#value);
    }
    throw new TypeError("Invalid filter kind");
  }

  /**
   * Convert the filter to a JSON object.
   */
  toJSON(): KvFilterJSON {
    switch (this.#kind) {
      case "and":
        return {
          kind: "and",
          filters: this.#filters.map((f) => f.toJSON()),
        };
      case "or":
        return {
          kind: "or",
          filters: this.#filters.map((f) => f.toJSON()),
        };
      case "where":
        assert(this.#property);
        assert(this.#operation);
        return {
          kind: "where",
          property: this.#property instanceof PropertyPath
            ? this.#property.toJSON()
            : this.#property,
          operation: this.#operation,
          value: valueToJSON(this.#value),
        };
      case "value":
        assert(this.#operation);
        return {
          kind: "value",
          operation: this.#operation,
          value: valueToJSON(this.#value),
        };
    }
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
    return new Filter("and", filters);
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
    return new Filter("or", filters);
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
    return new Filter("value", undefined, undefined, operation, value);
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
    return new Filter("where", undefined, property, operation, value);
  }

  /**
   * Parse a filter from a JSON object.
   */
  static parse(json: KvFilterJSON): Filter {
    switch (json.kind) {
      case "and":
        return Filter.and(...json.filters.map(Filter.parse));
      case "or":
        return Filter.or(...json.filters.map(Filter.parse));
      case "where":
        return Filter.where(
          Array.isArray(json.property)
            ? PropertyPath.from(json.property)
            : json.property,
          json.operation,
          toValue(json.value),
        );
      case "value":
        return Filter.value(json.operation, toValue(json.value));
    }
  }
}

const AsyncIterator = Object.getPrototypeOf(async function* () {}).constructor;

class QueryListIterator<T = unknown> extends AsyncIterator
  implements Deno.KvListIterator<T> {
  #iterator: Deno.KvListIterator<T>;
  #query: Filter[];

  get cursor(): string {
    return this.#iterator.cursor;
  }

  constructor(iterator: Deno.KvListIterator<T>, query: Filter[]) {
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
export class Query<T = unknown> implements QueryLike<T> {
  #kv: Deno.Kv;
  #selector: Deno.KvListSelector;
  #options: Deno.KvListOptions;
  #query: Filter[] = [];

  /**
   * The selector that is used to query the entries.
   */
  get selector(): Deno.KvListSelector {
    return { ...this.#selector };
  }

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
   * Resolves with an array of unique sub keys/prefixes for the provided prefix
   * along with the number of sub keys that match that prefix. The `count`
   * represents the number of sub keys, a value of `0` indicates that only the
   * exact key exists with no sub keys.
   *
   * This is useful when storing keys and values in a hierarchical/tree view,
   * where you are retrieving a list including counts and you want to know all
   * the unique _descendants_ of a key in order to be able to enumerate them.
   *
   * @example
   *
   * If you had the following keys stored in a datastore and the query matched
   * the keys:
   *
   * ```
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * And you would get the following results when using `.counts()`:
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const kv = await Deno.openKv();
   * console.log(await query(kv, { prefix: ["a"] }).counts());
   * // { key: ["a", "b"], count: 1 }
   * // { key: ["a", "d"], count: 2 }
   * await kv.close();
   * ```
   */
  counts(): Promise<UniqueCountElement[]> {
    return uniqueCount(this);
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
   * Return an array of keys that match the query.
   *
   * @example
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const kv = await Deno.openKv();
   * console.log(await query(kv, { prefix: ["hello"] }).keys());
   * await kv.close();
   * ```
   */
  keys(): Promise<Deno.KvKey[]> {
    return keys(this);
  }

  /**
   * Query a Deno KV store for keys and resolve with any matching keys
   * organized into a tree structure.
   *
   * Each child node indicates if it also has a value and any children of that
   * node.
   *
   * @example
   *
   * If you had the following keys stored in a datastore and the query matched
   * the values of all the entries:
   *
   * ```
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * And you would get the following results when using `.tree()`:
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const kv = await Deno.openKv();
   * console.log(await query(kv, { prefix: ["a"] }).tree());
   * // {
   * //   prefix: ["a"],
   * //   children: [
   * //     {
   * //       part: "b",
   * //       hasValue: true,
   * //       children: [{ part: "c", hasValue: true }]
   * //     }, {
   * //       part: "d",
   * //       children: [
   * //         { part: "e", hasValue: true },
   * //         { part: "f", hasValue: true }
   * //       ]
   * //     }
   * //   ]
   * // }
   * await kv.close();
   * ```
   */
  tree(): Promise<KeyTree> {
    return tree(this);
  }

  /**
   * Resolves with an array of unique sub keys/prefixes for the matched query.
   *
   * This is useful when storing keys and values in a hierarchical/tree view,
   * where you are retrieving a list and you want to know all the unique
   * _descendants_ of a key in order to be able to enumerate them.
   *
   * @example
   *
   * The following keys stored in a datastore that matched the query:
   *
   * ```
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * The following results when using `.unique()`:
   *
   * ```ts
   * import { query } from "@kitsonk/kv-toolbox/query";
   *
   * const kv = await Deno.openKv();
   * console.log(await query(kv, { prefix: ["a"] }).unique());
   * // ["a", "b"]
   * // ["a", "d"]
   * await kv.close();
   * ```
   */
  unique(): Promise<Deno.KvKey[]> {
    return unique(this);
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
  where(filter: Filter): this;
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
    propertyOrFilter: Filter | string | PropertyPath,
    operation?: Operation,
    value?: unknown | unknown[],
  ): this {
    if (propertyOrFilter instanceof Filter) {
      this.#query.push(propertyOrFilter);
    } else {
      assert(operation, "Operation is required");
      this.#query.push(Filter.where(propertyOrFilter, operation, value));
    }
    return this;
  }

  /**
   * Convert the query to a JSON object.
   */
  toJSON(): KvQueryJSON {
    return {
      selector: selectorToJSON(this.#selector),
      options: this.#options,
      filters: this.#query.map((f) => f.toJSON()),
    };
  }

  /**
   * Parse a query from an instance of {@linkcode Deno.Kv} and a JSON object.
   */
  static parse<T = unknown>(kv: Deno.Kv, json: KvQueryJSON): Query<T> {
    const query = new Query<T>(kv, toSelector(json.selector), json.options);
    query.#query = json.filters.map(Filter.parse);
    return query;
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
