import { assert } from "https://deno.land/std@0.214.0/assert/assert.ts";
export { delay } from "https://deno.land/std@0.214.0/async/delay.ts";
export { assert } from "https://deno.land/std@0.214.0/assert/assert.ts";
export { assertEquals } from "https://deno.land/std@0.214.0/assert/assert_equals.ts";
export {
  assertNotEquals,
} from "https://deno.land/std@0.214.0/assert/assert_not_equals.ts";
export { timingSafeEqual } from "https://deno.land/std@0.214.0/crypto/timing_safe_equal.ts";

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
