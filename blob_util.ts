/**
 * This is an internal module which contains some of the blob writing
 * functionality and is not part of the public API of kv-toolbox.
 *
 * @module
 */

import { type BatchedAtomicOperation } from "./batched_atomic.ts";

/**
 * When a blob entry was originally a {@linkcode Blob} or {@linkcode File} a
 * sub-entry will be set with the value of this meta data.
 */
export type BlobMeta = {
  kind: "blob";
  type: string;
} | {
  kind: "file";
  type: string;
  lastModified: number;
  name: string;
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

function writeBlob(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  blob: Blob,
  options?: { expireIn?: number },
): Promise<[count: number, operation: BatchedAtomicOperation]> {
  let meta: BlobMeta;
  if (blob instanceof File) {
    meta = {
      kind: "file",
      type: blob.type,
      lastModified: blob.lastModified,
      name: blob.name,
    };
  } else {
    meta = { kind: "blob", type: blob.type };
  }
  operation.set([...key, BLOB_META_KEY], meta, options);
  return writeStream(operation, key, blob.stream(), options);
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

export async function setBlob(
  operation: BatchedAtomicOperation,
  key: Deno.KvKey,
  blob: ArrayBufferLike | ReadableStream<Uint8Array> | Blob,
  itemCount: number,
  options?: { expireIn?: number },
) {
  let count;
  if (blob instanceof ReadableStream) {
    [count, operation] = await writeStream(operation, key, blob, options);
  } else if (blob instanceof Blob) {
    [count, operation] = await writeBlob(
      operation,
      key,
      blob,
      options,
    );
  } else {
    [count, operation] = writeArrayBuffer(operation, key, blob, 0, options);
  }
  operation = deleteKeys(operation, key, count, itemCount);
  return operation;
}
