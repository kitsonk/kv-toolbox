import { assert } from "jsr:@std/assert@~1/assert";
export { concat } from "jsr:@std/bytes@~1/concat";
export { delay } from "jsr:@std/async@~1/delay";
export { assert } from "jsr:@std/assert@~1/assert";
export { assertEquals } from "jsr:@std/assert@~1/equals";
export { assertNotEquals } from "jsr:@std/assert@~1/not-equals";
export { assertRejects } from "jsr:@std/assert@~1/rejects";
export { assertStrictEquals } from "jsr:@std/assert@~1/strict-equals";
export { assertThrows } from "jsr:@std/assert@~1/throws";
export { timingSafeEqual } from "jsr:@std/crypto@~1/timing-safe-equal";

let kv: { close(): void } | undefined;
let path: string | undefined;

export async function getPath() {
  return path = `${await Deno.makeTempDir()}/test.db`;
}

export async function setup() {
  return kv = await Deno.openKv(":memory:");
}

export function cleanup(): Promise<void> | void {
  if (path && path !== ":memory:") {
    return Deno.remove(path);
  }
}

export function teardown() {
  assert(kv);
  kv.close();
  return cleanup();
}
