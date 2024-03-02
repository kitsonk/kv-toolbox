/**
 * APIs for dealing with Deno KV keys.
 *
 * # equals()
 *
 * {@linkcode equals} compares if two {@linkcode Deno.KvKey}s are equal. Because
 * key parts can be an {@linkcode Uint8Array} they need to be compared deeply
 * in a way that avoids security exploits.
 *
 * **Example**
 *
 * ```ts
 * import { equals } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const keyA = ["a", "b"];
 * const keyB = ["a", "b"];
 * if (equals(keyA, keyB)) {
 *   console.log("keys match");
 * }
 * ```
 *
 * # keys()
 *
 * {@linkcode keys} is like Deno KV `.list()` except instead of returning an
 * async iterator of entries, it return an array of {@linkcode Deno.KvKey}s.
 *
 * # partEquals()
 *
 * {@linkcode partEquals} compares if two {@linkcode Deno.KvKeyPart}s are equal.
 * Because key parts can be an {@linkcode Uint8Array} they need to be compared
 * deeply in a way that avoids security exploits.
 *
 * **Example**
 *
 * ```ts
 * import { partEquals } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const keyA = ["a", "b"];
 * const keyB = ["a", "b"];
 * if (partEquals(keyA[0], keyB[0])) {
 *   console.log("keys match");
 * }
 * ```
 *
 * # startsWith()
 *
 * {@linkcode startsWith} determines if the `key` starts with the `prefix`
 * provided, returning `true` if does, otherwise `false`.
 *
 * **Example**
 *
 * ```ts
 * import { startsWith } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const key = ["a", "b"];
 * const prefix = ["a"];
 * if (equals(key, prefix)) {
 *   console.log("key starts with prefix");
 * }
 * ```
 *
 * # tree()
 *
 * {@linkcode tree} resolves with all keys (or keys that match the optional
 * `prefix`) organized into a tree structure.
 *
 * **Example**
 *
 * If you had the following keys stored in a datastore:
 *
 * ```ts
 * ["a", "b"]
 * ["a", "b", "c"]
 * ["a", "d", "e"]
 * ["a", "d", "f"]
 * ```
 *
 * And you would get the following results when using `unique()`:
 *
 * ```ts
 * import { unique } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const kv = await Deno.openKv();
 * console.log(await unique(kv, ["a"]));
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
 *
 * # unique()
 *
 * {@linkcode unique} resolves with an array of unique sub keys/prefixes for the
 * provided prefix. This is useful when storing keys and values in a
 * hierarchical/tree view, where you are retrieving a list and you want to know
 * all the unique _descendants_ of a key in order to be able to enumerate them.
 *
 * **Example**
 *
 * If you had the following keys stored in a datastore:
 *
 * ```ts
 * ["a", "b"]
 * ["a", "b", "c"]
 * ["a", "d", "e"]
 * ["a", "d", "f"]
 * ```
 *
 * And you would get the following results when using `unique()`:
 *
 * ```ts
 * import { unique } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const kv = await Deno.openKv();
 * console.log(await unique(kv, ["a"]));
 * // ["a", "b"]
 * // ["a", "d"]
 * await kv.close();
 * ```
 *
 * # uniqueCount()
 *
 * {@linkcode uniqueCount} resolves with an array of values which contain the
 * unique sub keys/prefixes for the provided prefix along with a count of how
 * many keys there are. This is useful when storing keys and values in a
 * hierarchical/tree view, where you are retrieving a list and you want to know
 * all the unique _descendants_ of a key (and the count of keys that match that
 * prefix) in order to be able to enumerate them or provide information about
 * them.
 *
 * @module
 */

import { timingSafeEqual } from "https://deno.land/std@0.217.0/crypto/timing_safe_equal.ts";

function addIfUnique(set: Set<Deno.KvKeyPart>, item: Uint8Array) {
  for (const i of set) {
    if (ArrayBuffer.isView(i) && timingSafeEqual(i, item)) {
      return;
    }
  }
  set.add(item);
}

function addOrIncrement(
  map: Map<Deno.KvKeyPart, number>,
  item: Uint8Array,
  increment: boolean,
) {
  for (const [k, v] of map) {
    if (ArrayBuffer.isView(k) && timingSafeEqual(k, item)) {
      map.set(k, increment ? v + 1 : v);
      return;
    }
  }
  map.set(item, increment ? 1 : 0);
}

/** Determines if one {@linkcode Deno.KvKeyPart} equals another. This is more
 * focused than just comparison as it compares `Uint8Array` parts in a way that
 * avoids potential code exploits.
 *
 * **Example**
 *
 * ```ts
 * import { partEquals } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const keyA = ["a", "b"];
 * const keyB = ["a", "b"];
 * if (partEquals(keyA[0], keyB[0])) {
 *   console.log("keys match");
 * }
 * ```
 */
export function partEquals(a: Deno.KvKeyPart, b: Deno.KvKeyPart): boolean {
  if (ArrayBuffer.isView(a)) {
    if (!ArrayBuffer.isView(b)) {
      return false;
    }
    if (!timingSafeEqual(a, b)) {
      return false;
    }
  } else if (a !== b) {
    return false;
  }
  return true;
}

/** Determines if one {@linkcode Deno.KvKey} equals another. This is more
 * focused than a deeply equals comparison and compares key parts that are
 * `Uint8Array` in a way that avoids potential code exploits.
 *
 * **Example**
 *
 * ```ts
 * import { equals } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const keyA = ["a", "b"];
 * const keyB = ["a", "b"];
 * if (equals(keyA, keyB)) {
 *   console.log("keys match");
 * }
 * ```
 */
export function equals(a: Deno.KvKey, b: Deno.KvKey): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    const partA = a[i];
    const partB = b[i];
    if (!partEquals(partA, partB)) {
      return false;
    }
  }
  return true;
}

/** Determines if one {@linkcode Deno.KvKey} matches the prefix of another.
 *
 * **Example**
 *
 * ```ts
 * import { startsWith } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const key = ["a", "b"];
 * const prefix = ["a"];
 * if (equals(key, prefix)) {
 *   console.log("key starts with prefix");
 * }
 * ```
 */
export function startsWith(key: Deno.KvKey, prefix: Deno.KvKey): boolean {
  if (prefix.length > key.length) {
    return false;
  }
  return equals(prefix, key.slice(0, prefix.length));
}

/** Return an array of keys that match the `selector` in the target `kv`
 * store.
 *
 * **Example**
 *
 * ```ts
 * import { keys } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const kv = await Deno.openKv();
 * console.log(await keys(kv, { prefix: ["hello"] }));
 * await kv.close();
 * ```
 */
export async function keys(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options?: Deno.KvListOptions,
): Promise<Deno.KvKey[]> {
  const list = kv.list(selector, options);
  const keys: Deno.KvKey[] = [];
  for await (const { key } of list) {
    keys.push(key);
  }
  return keys;
}

/** Resolves with an array of unique sub keys/prefixes for the provided prefix.
 *
 * This is useful when storing keys and values in a hierarchical/tree view,
 * where you are retrieving a list and you want to know all the unique
 * _descendants_ of a key in order to be able to enumerate them.
 *
 * For example if you had the following keys stored in a datastore:
 *
 * ```ts
 * ["a", "b"]
 * ["a", "b", "c"]
 * ["a", "d", "e"]
 * ["a", "d", "f"]
 * ```
 *
 * And you would get the following results when using `unique()`:
 *
 * ```ts
 * import { unique } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const kv = await Deno.openKv();
 * console.log(await unique(kv, ["a"]));
 * // ["a", "b"]
 * // ["a", "d"]
 * await kv.close();
 * ```
 *
 * If you omit a `prefix`, all unique root keys are resolved.
 */
export async function unique(
  kv: Deno.Kv,
  prefix: Deno.KvKey = [],
  options?: Deno.KvListOptions,
): Promise<Deno.KvKey[]> {
  const list = kv.list({ prefix }, options);
  const prefixLength = prefix.length;
  const prefixes = new Set<Deno.KvKeyPart>();
  for await (const { key } of list) {
    if (key.length <= prefixLength) {
      throw new TypeError(`Unexpected key length of ${key.length}.`);
    }
    const part = key[prefixLength];
    if (ArrayBuffer.isView(part)) {
      addIfUnique(prefixes, part);
    } else {
      prefixes.add(part);
    }
  }
  return [...prefixes].map((part) => [...prefix, part]);
}

/** Resolves with an array of unique sub keys/prefixes for the provided prefix
 * along with the number of sub keys that match that prefix. The `count`
 * represents the number of sub keys, a value of `0` indicates that only the
 * exact key exists with no sub keys.
 *
 * This is useful when storing keys and values in a hierarchical/tree view,
 * where you are retrieving a list including counts and you want to know all the
 * unique _descendants_ of a key in order to be able to enumerate them.
 *
 * For example if you had the following keys stored in a datastore:
 *
 * ```ts
 * ["a", "b"]
 * ["a", "b", "c"]
 * ["a", "d", "e"]
 * ["a", "d", "f"]
 * ```
 *
 * And you would get the following results when using `unique()`:
 *
 * ```ts
 * import { uniqueCount } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const kv = await Deno.openKv();
 * console.log(await uniqueCount(kv, ["a"]));
 * // { key: ["a", "b"], count: 1 }
 * // { key: ["a", "d"], count: 2 }
 * await kv.close();
 * ```
 *
 * If you omit a `prefix`, all unique root keys are resolved.
 */
export async function uniqueCount(
  kv: Deno.Kv,
  prefix: Deno.KvKey = [],
  options?: Deno.KvListOptions,
): Promise<{ key: Deno.KvKey; count: number }[]> {
  const list = kv.list({ prefix }, options);
  const prefixLength = prefix.length;
  const prefixCounts = new Map<Deno.KvKeyPart, number>();
  for await (const { key } of list) {
    if (key.length <= prefixLength) {
      throw new TypeError(`Unexpected key length of ${key.length}.`);
    }
    const part = key[prefixLength];
    if (ArrayBuffer.isView(part)) {
      addOrIncrement(prefixCounts, part, key.length > (prefixLength + 1));
    } else {
      if (!prefixCounts.has(part)) {
        prefixCounts.set(part, 0);
      }
      if (key.length > (prefixLength + 1)) {
        prefixCounts.set(part, prefixCounts.get(part)! + 1);
      }
    }
  }
  return [...prefixCounts].map(([part, count]) => ({
    key: [...prefix, part],
    count,
  }));
}

/** A node of a query of a Deno KV store, providing the key part and any
 * children. */
interface KeyTreeNode {
  /** The unique {@linkcode Deno.KvKeyPart} that represents the node. */
  part: Deno.KvKeyPart;
  /** Indicates if the key represented by the node has a value. This property
   * is only present if `true`. */
  hasValue?: true;
  /** An array of children nodes, if any, associated with the key part. */
  children?: KeyTreeNode[];
}

/** The root node of a key query of the Deno KV store where the keys are
 * organized into a tree structure. */
export interface KeyTree {
  /** The prefix, if any, of the tree structure. If there is no prefix, then
   * this is the root of the Deno KV store. */
  prefix?: Deno.KvKey;
  /** An array of children nodes, if any, associated with the root of the
   * query. */
  children?: KeyTreeNode[];
}

/** Query a Deno KV store for keys and resolve with any matching keys
 * organized into a tree structure.
 *
 * The root of the tree will be either the root of Deno KV store or if a prefix
 * is supplied, keys that match the prefix. Each child node indicates if it
 * also has a value and any children of that node.
 *
 * **Example**
 *
 * If you had the following keys stored in a datastore:
 *
 * ```ts
 * ["a", "b"]
 * ["a", "b", "c"]
 * ["a", "d", "e"]
 * ["a", "d", "f"]
 * ```
 *
 * And you would get the following results when using `unique()`:
 *
 * ```ts
 * import { unique } from "jsr:@kitsonk/kv-toolbox/keys";
 *
 * const kv = await Deno.openKv();
 * console.log(await unique(kv, ["a"]));
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
export async function tree(
  kv: Deno.Kv,
  prefix: Deno.KvKey = [],
  options?: Deno.KvListOptions,
): Promise<KeyTree> {
  const root: KeyTree = prefix.length ? { prefix: [...prefix] } : {};
  const prefixLength = prefix.length;
  const list = kv.list({ prefix }, options);
  for await (const { key } of list) {
    if (!root.children) {
      root.children = [];
    }
    const suffix: Deno.KvKey = key.slice(prefixLength);
    let children = root.children;
    let node: KeyTreeNode | undefined;
    for (const part of suffix) {
      if (node) {
        if (!node.children) {
          node.children = [];
        }
        children = node.children;
      }
      const child = children.find(({ part: p }) => partEquals(part, p));
      if (child) {
        node = child;
      } else {
        node = { part };
        children.push(node);
      }
    }
    if (node) {
      node.hasValue = true;
    }
  }
  return root;
}
