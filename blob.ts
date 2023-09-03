import { batchedAtomic, type BatchedAtomicOperation } from "./batchedAtomic.ts";
import { keys } from "./keys.ts";

const BATCH_SIZE = 10;
const CHUNK_SIZE = 63_000;
const BLOB_KEY = "__kv_toolbox_blob__";

function asSteam(
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

function deleteKeys(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  count: number,
  length: number,
): BatchedAtomicOperation {
  while (++count <= length) {
    operation.delete([...key, BLOB_KEY, count]);
  }
  return operation;
}

function writeArrayBuffer(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  blob: ArrayBufferLike,
  start = 0,
  options?: { expireIn?: number },
): [count: number, operation: BatchedAtomicOperation] {
  const buffer = new Uint8Array(blob);
  let offset = 0;
  let count = start;
  while (buffer.byteLength > offset) {
    count++;
    const chunk = buffer.subarray(offset, offset + CHUNK_SIZE);
    operation.set([...key, BLOB_KEY, count], chunk, options);
    offset += CHUNK_SIZE;
  }
  return [count, operation];
}

async function writeStream(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  stream: ReadableStream<Uint8Array>,
  options?: { expireIn?: number },
): Promise<[count: number, operation: BatchedAtomicOperation]> {
  let start = 0;
  for await (const chunk of stream) {
    [start, operation] = writeArrayBuffer(
      operation,
      key,
      chunk,
      start,
      options,
    );
  }
  return [start, operation];
}

/** Remove/delete a binary object from the store with a given key that has been
 * {@linkcode set}.
 *
 * ### Example
 *
 * ```ts
 * import { remove } from "https://deno.land/x/kv-tools/blob.ts";
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
    let op = kv.atomic();
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
 * ### Example
 *
 * ```ts
 * import { get } from "https://deno.land/x/kv-tools/blob.ts";
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
 * ### Example
 *
 * ```ts
 * import { get } from "https://deno.land/x/kv-tools/blob.ts";
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
    ? asSteam(kv, key, options)
    : asUint8Array(kv, key, options);
}

/** Set the blob value in the provided {@linkcode Deno.Kv} with the provided
 * key. The blob can be any array buffer like structure or a byte
 * {@linkcode ReadableStream}.
 *
 * The function chunks up the blob into parts which deno be stored in Deno KV
 * and should be retrieved back out using the {@linkcode get} function.
 *
 * ### Example
 *
 * ```ts
 * import { set } from "https://deno.land/x/kv-tools/blob.ts";
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
  blob: ArrayBufferLike | ReadableStream<Uint8Array>,
  options?: { expireIn?: number },
): Promise<void> {
  const items = await keys(kv, { prefix: [...key, BLOB_KEY] });
  let operation = batchedAtomic(kv);
  let count;
  if (blob instanceof ReadableStream) {
    [count, operation] = await writeStream(operation, key, blob, options);
  } else {
    [count, operation] = writeArrayBuffer(operation, key, blob, 0, options);
  }
  operation = deleteKeys(operation, key, count, items.length);
  await operation.commit();
}
