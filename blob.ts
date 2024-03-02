/**
 * A set of APIs for storing arbitrarily sized blobs in Deno KV. Currently Deno
 * KV has a limit of key values being 64k. The {@linkcode set} function breaks
 * down a blob into chunks and manages sub-keys to store the complete value. The
 * {@linkcode get} function reverses that process, and {@linkcode remove}
 * function will delete the key, sub-keys and values.
 *
 * **Example**
 *
 * ```ts
 * import { get, remove, set } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const data = new TextEncoder().encode("hello deno!");
 * await set(kv, ["hello"], data);
 * const ab = await get(kv, ["hello"]);
 * // do something with ab
 * await remove(kv, ["hello"]);
 * await kv.close();
 * ```
 *
 * @module
 */

import { batchedAtomic } from "./batched_atomic.ts";
import { BLOB_KEY, CHUNK_SIZE, setBlob } from "./blob_util.ts";
import { keys } from "./keys.ts";

const BATCH_SIZE = 10;

function asStream(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
) {
  let list: Deno.KvListIterator<Uint8Array> | null = null;
  return new ReadableStream({
    type: "bytes",
    autoAllocateChunkSize: CHUNK_SIZE,
    async pull(controller) {
      if (!list) {
        return controller.error(new Error("Internal error - list not set"));
      }
      const next = await list.next();
      if (next.value?.value) {
        if (next.value.value instanceof Uint8Array) {
          controller.enqueue(next.value.value);
        } else {
          controller.error(new TypeError("KV value is not a Uint8Array."));
        }
      }
      if (next.done) {
        controller.close();
      }
    },
    start() {
      list = kv.list<Uint8Array>({ prefix: [...key, BLOB_KEY] }, {
        ...options,
        batchSize: BATCH_SIZE,
      });
    },
  });
}

async function asUint8Array(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
): Promise<Uint8Array | null> {
  const list = kv.list<Uint8Array>({ prefix: [...key, BLOB_KEY] }, {
    ...options,
    batchSize: BATCH_SIZE,
  });
  let found = false;
  let value = new Uint8Array();
  for await (const item of list) {
    if (item.value) {
      found = true;
      if (!(item.value instanceof Uint8Array)) {
        throw new TypeError("KV value is not a Uint8Array.");
      }
      const v = new Uint8Array(value.length + item.value.length);
      v.set(value, 0);
      v.set(item.value, value.length);
      value = v;
    }
  }
  return found ? value : null;
}

/** Remove/delete a binary object from the store with a given key that has been
 * {@linkcode set}.
 *
 * **Example**
 *
 * ```ts
 * import { remove } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * await remove(kv, ["hello"]);
 * await kv.close();
 * ```
 */
export async function remove(kv: Deno.Kv, key: Deno.KvKey): Promise<void> {
  const parts = await keys(kv, { prefix: [...key, BLOB_KEY] }, {
    batchSize: BATCH_SIZE,
  });
  if (parts.length) {
    let op = batchedAtomic(kv);
    for (const key of parts) {
      op = op.delete(key);
    }
    await op.commit();
  }
}

/** Retrieve a binary object from the store with a given key that has been
 * {@linkcode set}.
 *
 * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
 * returned to read the blob in chunks of {@linkcode Uint8Array}, otherwise the
 * function resolves with a single {@linkcode Uint8Array}.
 *
 * **Example**
 *
 * ```ts
 * import { get } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const stream = await get(kv, ["hello"], { stream: true });
 * for await (const chunk of stream) {
 *   // do something with chunk
 * }
 * await kv.close();
 * ```
 */
export function get(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined; stream: true },
): ReadableStream<Uint8Array>;
/** Retrieve a binary object from the store with a given key that has been
 * {@linkcode set}.
 *
 * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
 * returned to read the blob in chunks of {@linkcode Uint8Array}, otherwise the
 * function resolves with a single {@linkcode Uint8Array}.
 *
 * **Example**
 *
 * ```ts
 * import { get } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const ab = await get(kv, ["hello"]);
 * // do something with ab
 * await kv.close();
 * ```
 */
export function get(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options?: {
    consistency?: Deno.KvConsistencyLevel | undefined;
    stream?: boolean;
  },
): Promise<Uint8Array | null>;
export function get(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: {
    consistency?: Deno.KvConsistencyLevel | undefined;
    stream?: boolean;
  } = {},
): ReadableStream<Uint8Array> | Promise<Uint8Array | null> {
  return options.stream
    ? asStream(kv, key, options)
    : asUint8Array(kv, key, options);
}

/** Set the blob value in the provided {@linkcode Deno.Kv} with the provided
 * key. The blob can be any array buffer like structure, a byte
 * {@linkcode ReadableStream}, or a {@linkcode Blob}.
 *
 * The function chunks up the blob into parts which deno be stored in Deno KV
 * and should be retrieved back out using the {@linkcode get} function.
 *
 * Optionally an `expireIn` option can be specified to set a time-to-live
 * (TTL) for the key. The TTL is specified in milliseconds, and the key will
 * be deleted from the database at earliest after the specified number of
 * milliseconds have elapsed. Once the specified duration has passed, the
 * key may still be visible for some additional time. If the `expireIn`
 * option is not specified, the key will not expire.
 *
 * **Example**
 *
 * ```ts
 * import { set } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const blob = new TextEncoder().encode("hello deno!");
 * await set(kv, ["hello"], blob);
 * // do something with ab
 * await kv.close();
 * ```
 */
export async function set(
  kv: Deno.Kv,
  key: Deno.KvKey,
  blob: ArrayBufferLike | ReadableStream<Uint8Array> | Blob,
  options?: { expireIn?: number },
): Promise<void> {
  const items = await keys(kv, { prefix: [...key, BLOB_KEY] });
  let operation = batchedAtomic(kv);
  operation = await setBlob(operation, key, blob, items.length, options);
  await operation.commit();
}
