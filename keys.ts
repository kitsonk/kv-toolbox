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
