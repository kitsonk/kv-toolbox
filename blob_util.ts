/**
 * This is an internal module which contains some of the blob writing
 * functionality and is not part of the public API of kv-toolbox.
 *
 * @module
 */

import type { BatchedAtomicOperation } from "./batched_atomic.ts";
import { keys } from "./keys.ts";

/**
 * When a blob entry was originally a {@linkcode Blob} or {@linkcode File} a
 * sub-entry will be set with the value of this meta data.
 */
export type BlobMeta = {
  kind: "blob";
  encrypted?: boolean;
  type: string;
  size?: number;
} | {
  kind: "file";
  encrypted?: boolean;
  type: string;
  lastModified: number;
  name: string;
  size?: number;
} | {
  kind: "buffer";
  encrypted?: boolean;
  size?: number;
};

/**
 * When there are parts of a blob, this key will be set as a sub-key of the blob
 * blob entry, which will have additional sub-keys with the parts of the blob
 * stored as {@linkcode Uint8Array} with a key of an incrementing number.
 */
export const BLOB_KEY = "__kv_toolbox_blob__";
/**
 * If there is meta data associated with a blob entry, like for something that
 * was originally a {@linkcode Blob} or {@linkcode File}, then this will be set
 * as a sub-key of that blob key with a value of the meta data.
 */
export const BLOB_META_KEY = "__kv_toolbox_meta__";
export const CHUNK_SIZE = 63_000;
export const BATCH_SIZE = 10;

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
  blob: ArrayBufferLike | ArrayBufferView,
  start = 0,
  options?: { expireIn?: number },
): [count: number, operation: BatchedAtomicOperation] {
  const buffer = new Uint8Array(ArrayBuffer.isView(blob) ? blob.buffer : blob);
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

function writeBlob(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  blob: Blob,
  options: { expireIn?: number; encrypted?: boolean } = {},
): Promise<[count: number, operation: BatchedAtomicOperation, size: number]> {
  let meta: BlobMeta;
  if (blob instanceof File) {
    meta = {
      kind: "file",
      type: blob.type,
      lastModified: blob.lastModified,
      name: blob.name,
      size: blob.size,
    };
  } else {
    meta = {
      kind: "blob",
      type: blob.type,
      size: blob.size,
    };
  }
  if (options.encrypted) {
    meta.encrypted = options.encrypted;
  }
  operation.set([...key, BLOB_META_KEY], meta, options);
  return writeStream(operation, key, blob.stream(), options);
}

async function writeStream(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  stream: ReadableStream<Uint8Array>,
  options?: { expireIn?: number },
): Promise<[count: number, operation: BatchedAtomicOperation, size: number]> {
  let start = 0;
  let size = 0;
  for await (const chunk of stream) {
    size += chunk.byteLength;
    [start, operation] = writeArrayBuffer(
      operation,
      key,
      chunk,
      start,
      options,
    );
  }
  return [start, operation, size];
}

export function asMeta(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
): Promise<Deno.KvEntryMaybe<BlobMeta>> {
  return kv.get<BlobMeta>([...key, BLOB_META_KEY], options);
}

export async function asUint8Array(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
): Promise<Uint8Array | null> {
  const prefix = [...key, BLOB_KEY];
  const prefixLength = prefix.length;
  const list = kv.list<Uint8Array>({ prefix }, {
    ...options,
    batchSize: BATCH_SIZE,
  });
  let found = false;
  let value = new Uint8Array();
  let i = 1;
  for await (const item of list) {
    if (
      item.value && item.key.length === prefixLength + 1 &&
      item.key[prefixLength] === i
    ) {
      i++;
      found = true;
      if (!(item.value instanceof Uint8Array)) {
        throw new TypeError("KV value is not a Uint8Array.");
      }
      const v = new Uint8Array(value.length + item.value.length);
      v.set(value, 0);
      v.set(item.value, value.length);
      value = v;
    } else {
      break;
    }
  }
  return found ? value : null;
}

export function asStream(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
) {
  const prefix = [...key, BLOB_KEY];
  const prefixLength = prefix.length;
  let i = 1;
  let list: Deno.KvListIterator<Uint8Array> | null = null;
  return new ReadableStream({
    type: "bytes",
    autoAllocateChunkSize: CHUNK_SIZE,
    async pull(controller) {
      if (!list) {
        return controller.error(new Error("Internal error - list not set"));
      }
      const next = await list.next();
      if (
        next.value && next.value.value &&
        next.value.key.length === prefixLength + 1 &&
        next.value.key[prefixLength] === i
      ) {
        i++;
        if (next.value.value instanceof Uint8Array) {
          controller.enqueue(next.value.value);
        } else {
          controller.error(new TypeError("KV value is not a Uint8Array."));
        }
      } else {
        controller.close();
      }
      if (next.done) {
        controller.close();
      }
    },
    start() {
      list = kv.list<Uint8Array>({ prefix }, {
        ...options,
        batchSize: BATCH_SIZE,
      });
    },
  });
}

export async function setBlob(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  blob: ArrayBufferLike | ArrayBufferView | ReadableStream<Uint8Array> | Blob,
  itemCount: number,
  options: { expireIn?: number; encrypted?: boolean } = {},
) {
  let count;
  let size;
  if (blob instanceof ReadableStream) {
    [count, operation, size] = await writeStream(operation, key, blob, options);
    const meta: BlobMeta = { kind: "buffer", size };
    if (options.encrypted) {
      meta.encrypted = options.encrypted;
    }
    operation = operation.set([...key, BLOB_META_KEY], meta);
  } else if (blob instanceof Blob) {
    [count, operation] = await writeBlob(
      operation,
      key,
      blob,
      options,
    );
  } else if (
    ArrayBuffer.isView(blob) || blob instanceof ArrayBuffer ||
    blob instanceof SharedArrayBuffer
  ) {
    [count, operation] = writeArrayBuffer(operation, key, blob, 0, options);
    const meta: BlobMeta = { kind: "buffer", size: blob.byteLength };
    if (options.encrypted) {
      meta.encrypted = options.encrypted;
    }
    operation = operation.set([...key, BLOB_META_KEY], meta);
  } else {
    throw new TypeError(
      "Blob must be typed array, array buffer, ReadableStream, Blob, or File",
    );
  }
  operation = deleteKeys(operation, key, count, itemCount);
  return operation;
}

export async function removeBlob(kv: Deno.Kv, key: Deno.KvKey) {
  const parts = await keys(kv, { prefix: [...key, BLOB_KEY] });
  if (parts.length) {
    let op = kv.atomic().delete([...key, BLOB_META_KEY]);
    for (const key of parts) {
      op = op.delete(key);
    }
    await op.commit();
  }
}
