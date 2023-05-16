import { timingSafeEqual } from "https://deno.land/std@0.186.0/crypto/timing_safe_equal.ts";

function addIfUnique(set: Set<Deno.KvKeyPart>, item: Uint8Array) {
  for (const i of set) {
    if (ArrayBuffer.isView(i) && timingSafeEqual(i, item)) {
      return;
    }
  }
  set.add(item);
}

/** Return an array of keys that match the `selector` in the target `kv`
 * store.
 *
 * ### Example
 *
 * ```ts
 * import { keys } from "https://deno.land/x/kv-tools/keys.ts";
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
 * _descendents_ of a key in order to be able to enumerate them.
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
 * import { unique } from "https://deno.land/x/kv-tools/keys.ts";
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
