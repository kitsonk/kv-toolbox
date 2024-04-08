import { assert } from "jsr:@std/assert@0.220/assert";
export { concat } from "jsr:@std/bytes@0.220/concat";
export { delay } from "jsr:@std/async@0.220/delay";
export { assert } from "jsr:@std/assert@0.220/assert";
export { assertEquals } from "jsr:@std/assert@0.220/assert_equals";
export { assertNotEquals } from "jsr:@std/assert@0.220/assert_not_equals";
export { assertRejects } from "jsr:@std/assert@0.220/assert_rejects";
export { assertStrictEquals } from "jsr:@std/assert@0.220/assert_strict_equals";
export { timingSafeEqual } from "jsr:@std/crypto@0.220/timing_safe_equal";

let kv: Deno.Kv | undefined;
let path: string | undefined;

export async function setup(): Promise<Deno.Kv> {
  path = `${await Deno.makeTempDir()}/test.db`;
  return kv = await Deno.openKv(path);
}

export async function teardown() {
  assert(kv);
  kv.close();
  assert(path);
  await Deno.remove(path);
}
