import { timingSafeEqual } from "https://deno.land/std@0.203.0/crypto/timing_safe_equal.ts";

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

/** Determines if one {@linkcode Deno.KvKey} equals another. This is more
 * focused than a deeply equals comparison and compares key parts that are
 * `Uint8Array` in a way that avoids potential code exploits.
 *
 * ### Example
 *
 * ```ts
 * import { equals } from "https://deno.land/x/kv_toolbox/keys.ts";
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
    if (ArrayBuffer.isView(partA)) {
      if (!ArrayBuffer.isView(partB)) {
        return false;
      }
      if (!timingSafeEqual(partA, partB)) {
        return false;
      }
    } else if (partA !== partB) {
      return false;
    }
  }
  return true;
}

/** Determines if one {@linkcode Deno.KvKey} matches the prefix of another.
 *
 * ### Example
 *
 * ```ts
 * import { startsWith } from "https://deno.land/x/kv_toolbox/keys.ts";
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
 * ### Example
 *
 * ```ts
 * import { keys } from "https://deno.land/x/kv_toolbox/keys.ts";
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
 * import { unique } from "https://deno.land/x/kv_toolbox/keys.ts";
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
 * import { uniqueCount } from "https://deno.land/x/kv_toolbox/keys.ts";
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
