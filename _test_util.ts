import { assert } from "jsr:@std/assert@0.225/assert";
export { concat } from "jsr:@std/bytes@0.224/concat";
export { delay } from "jsr:@std/async@0.224/delay";
export { assert } from "jsr:@std/assert@0.225/assert";
export { assertEquals } from "jsr:@std/assert@0.225/assert-equals";
export { assertNotEquals } from "jsr:@std/assert@0.225/assert-not-equals";
export { assertRejects } from "jsr:@std/assert@0.225/assert-rejects";
export { assertStrictEquals } from "jsr:@std/assert@0.225/assert-strict-equals";
export { timingSafeEqual } from "jsr:@std/crypto@0.224/timing-safe-equal";

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
