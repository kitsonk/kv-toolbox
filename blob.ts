/**
 * A set of APIs for storing arbitrarily sized blobs in Deno KV. Currently Deno
 * KV has a limit of key values being 64k.
 *
 * The {@linkcode set} function breaks down a blob into chunks and manages
 * sub-keys to store the complete value, including preserving meta data
 * associated with {@linkcode Blob} and {@linkcode File} instances.
 *
 * The {@linkcode get}, {@linkcode getAsBlob} and {@linkcode getAsStream}
 * functions reverse that process, and {@linkcode remove} function will delete
 * the key, sub-keys and values.
 *
 * In addition, if a {@linkcode Blob} or {@linkcode File} is provided on set,
 *
 * @example Basic usage
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
 * @example Setting and getting `File`s
 *
 * ```ts
 * import { getAsBlob, remove, set } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * // assume this is form data submitted as a `Request`
 * const body = new FormData();
 * for (const [name, value] of body) {
 *   if (value instanceof File) {
 *     await set(kv, ["files", name], value);
 *   }
 * }
 * // and then later
 * const file = await getAsBlob(kv, ["file", "image"]);
 * // now the `File` is restored and can be processed
 * await remove(kv, ["file", "image"]);
 * await kv.close();
 * ```
 *
 * @module
 */

import {
  decodeBase64Url,
  encodeBase64Url,
} from "jsr:@std/encoding@0.220/base64url";
export { concat } from "jsr:@std/bytes@0.220/concat";

import { batchedAtomic } from "./batched_atomic.ts";
import {
  BLOB_KEY,
  BLOB_META_KEY,
  type BlobMeta,
  CHUNK_SIZE,
  setBlob,
} from "./blob_util.ts";
import { keys } from "./keys.ts";
import { concat } from "./_test_util.ts";

export { BLOB_KEY, BLOB_META_KEY, type BlobMeta } from "./blob_util.ts";

/** An interface to represent a blob value as JSON. */
export type BlobJSON = BlobBlobJSON | BlobBufferJSON | BlobFileJSON;

/** An interface to represent a {@linkcode Blob} value as JSON. */
export interface BlobBlobJSON {
  meta: {
    kind: "blob";
    type: string;
    size?: number;
  };
  parts: string[];
}

/** An interface to represent a array buffer or typed array value as JSON. */
export interface BlobBufferJSON {
  meta: { kind: "buffer"; size?: number };
  parts: string[];
}

/** An interface to represent a {@linkcode File} value as JSON. */
export interface BlobFileJSON {
  meta: {
    kind: "file";
    type: string;
    lastModified: number;
    name: string;
    size?: number;
  };
  parts: string[];
}

const BATCH_SIZE = 10;

async function asBlob(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
): Promise<File | Blob | null> {
  const list = kv.list<Uint8Array>({ prefix: [...key, BLOB_KEY] }, {
    ...options,
    batchSize: BATCH_SIZE,
  });
  let found = false;
  const parts: Uint8Array[] = [];
  for await (const item of list) {
    if (item.value) {
      found = true;
      if (!(item.value instanceof Uint8Array)) {
        throw new TypeError("KV value is not a Uint8Array.");
      }
      parts.push(item.value);
    }
  }
  if (!found) {
    return null;
  }
  const maybeMeta = await kv.get<BlobMeta>([...key, BLOB_META_KEY]);
  if (maybeMeta.value) {
    const { value } = maybeMeta;
    if (value.kind === "file") {
      return new File(parts, value.name, {
        lastModified: value.lastModified,
        type: value.type,
      });
    }
    if (value.kind === "blob") {
      return new Blob(parts, { type: value.type });
    }
  }
  return new Blob(parts);
}

async function asJSON(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
): Promise<BlobJSON | null> {
  const list = kv.list<Uint8Array>({ prefix: [...key, BLOB_KEY] }, {
    ...options,
    batchSize: BATCH_SIZE,
  });
  let found = false;
  const parts: Uint8Array[] = [];
  for await (const item of list) {
    if (item.value) {
      found = true;
      if (!(item.value instanceof Uint8Array)) {
        throw new TypeError("KV value is not a Uint8Array");
      }
      parts.push(item.value);
    }
  }
  if (!found) {
    return null;
  }
  const json: BlobJSON = {
    meta: { kind: "buffer" },
    parts: parts.map(encodeBase64Url),
  };
  // deno-lint-ignore no-explicit-any
  const maybeMeta = await kv.get<any>([...key, BLOB_META_KEY], options);
  if (maybeMeta.value) {
    json.meta = maybeMeta.value;
  }
  return json;
}

async function asMeta(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
): Promise<BlobMeta | null> {
  const maybeEntry = await kv.get<BlobMeta>([...key, BLOB_META_KEY], options);
  return maybeEntry.value;
}

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

function toParts(blob: ArrayBufferLike): string[] {
  const buffer = new Uint8Array(blob);
  const parts: string[] = [];
  let offset = 0;
  while (buffer.byteLength > offset) {
    parts.push(encodeBase64Url(buffer.subarray(offset, offset + CHUNK_SIZE)));
    offset += CHUNK_SIZE;
  }
  return parts;
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
    let op = batchedAtomic(kv).delete([...key, BLOB_META_KEY]);
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
 * returned to read the blob in chunks of {@linkcode Uint8Array}
 *
 * When setting the option `blob` to `true`, the promise resolves with a
 * {@linkcode Blob}, {@linkcode File}, or `null`. If the original file had been
 * a {@linkcode File} or {@linkcode Blob} it the resolved value will reflect
 * that original value including its properties. If it was not, it will be a
 * {@linkcode Blob} with a type of `""`.
 *
 * Otherwise the function resolves with a single {@linkcode Uint8Array} or
 * `null`.
 *
 * @example
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
 * returned to read the blob in chunks of {@linkcode Uint8Array}
 *
 * When setting the option `blob` to `true`, the promise resolves with a
 * {@linkcode Blob}, {@linkcode File}, or `null`. If the original file had been
 * a {@linkcode File} or {@linkcode Blob} it the resolved value will reflect
 * that original value including its properties. If it was not, it will be a
 * {@linkcode Blob} with a type of `""`.
 *
 * Otherwise the function resolves with a single {@linkcode Uint8Array} or
 * `null`.
 *
 * @example
 *
 * ```ts
 * import { get } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const blob = await get(kv, ["hello"], { blob: true });
 * // do something with blob
 * await kv.close();
 * ```
 */
export function get(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined; blob: true },
): Promise<File | Blob | null>;
/** Retrieve a binary object from the store with a given key that has been
 * {@linkcode set}.
 *
 * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
 * returned to read the blob in chunks of {@linkcode Uint8Array}
 *
 * When setting the option `blob` to `true`, the promise resolves with a
 * {@linkcode Blob}, {@linkcode File}, or `null`. If the original file had been
 * a {@linkcode File} or {@linkcode Blob} it the resolved value will reflect
 * that original value including its properties. If it was not, it will be a
 * {@linkcode Blob} with a type of `""`.
 *
 * Otherwise the function resolves with a single {@linkcode Uint8Array} or
 * `null`.
 *
 * @example
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
    blob?: boolean;
    stream?: boolean;
  },
): Promise<Uint8Array | null>;
export function get(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: {
    consistency?: Deno.KvConsistencyLevel | undefined;
    blob?: boolean;
    stream?: boolean;
  } = {},
):
  | ReadableStream<Uint8Array>
  | Promise<Uint8Array | null>
  | Promise<File | Blob | null> {
  return options.stream
    ? asStream(kv, key, options)
    : options.blob
    ? asBlob(kv, key, options)
    : asUint8Array(kv, key, options);
}

/**
 * Retrieve a binary object from the store as a {@linkcode Blob} or
 * {@linkcode File} that has been previously {@linkcode set}.
 *
 * If the object set was originally a {@linkcode Blob} or {@linkcode File} the
 * function will resolve with an instance of {@linkcode Blob} or
 * {@linkcode File} with the same properties as the original.
 *
 * If it was some other form of binary data, it will be an instance of
 * {@linkcode Blob} with an empty `.type` property.
 *
 * If there is no corresponding entry, the function will resolve to `null`.
 *
 * @example Getting a value
 *
 * ```ts
 * import { getAsBlob } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const blob = await getAsBlob(kv, ["hello"]);
 * // do something with blob
 * await kv.close();
 * ```
 */
export function getAsBlob(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
): Promise<Blob | File | null> {
  return asBlob(kv, key, options);
}

/**
 * Retrieve a binary object from the store as an object which which be safely
 * converted into a JSON string.
 *
 * If there is no corresponding entry, the promise will resolve with a `null`.
 *
 * @example Getting a value
 *
 * ```ts
 * import { getAsJSON } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const json = JSON.stringify(await getAsJSON(kv, ["hello"]));
 * await kv.close();
 * ```
 */
export function getAsJSON(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
): Promise<BlobJSON | null> {
  return asJSON(kv, key, options);
}

/**
 * Retrieve a binary object's meta data from the store.
 *
 * If there is no meta data available, `null` will be resolved.
 *
 * @example Getting meta data
 *
 * ```ts
 * import { getMeta } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const maybeMeta = await getMeta(kv, ["hello"]));
 * await kv.close();
 * ```
 */
export function getMeta(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
): Promise<BlobMeta | null> {
  return asMeta(kv, key, options);
}

/**
 * Retrieve a binary object from the store as a byte {@linkcode ReadableStream}.
 *
 * If there is no corresponding entry, the stream will provide no chunks.
 *
 * @example Getting a value
 *
 * ```ts
 * import { getAsStream } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const stream = await getAsStream(kv, ["hello"]);
 * // do something with stream
 * await kv.close();
 * ```
 */
export function getAsStream(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
): ReadableStream<Uint8Array> {
  return asStream(kv, key, options);
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
 * @example Setting a `Uint8Array`
 *
 * ```ts
 * import { set } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const blob = new TextEncoder().encode("hello deno!");
 * await set(kv, ["hello"], blob);
 * await kv.close();
 * ```
 *
 * @example Setting a `Blob`
 *
 * ```ts
 * import { set } from "jsr:@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const blob = new Blob(
 *   [new TextEncoder().encode("hello deno!")],
 *   { type: "text/plain" },
 * );
 * await set(kv, ["hello"], blob);
 * await kv.close();
 * ```
 */
export async function set(
  kv: Deno.Kv,
  key: Deno.KvKey,
  blob: ArrayBufferLike | ReadableStream<Uint8Array> | Blob | File,
  options?: { expireIn?: number },
): Promise<void> {
  const items = await keys(kv, { prefix: [...key, BLOB_KEY] });
  let operation = batchedAtomic(kv);
  operation = await setBlob(operation, key, blob, items.length, options);
  await operation.commit();
}

/**
 * Convert a typed array, array buffer, {@linkcode Blob} or {@linkcode File}
 * into a form that can be converted into a JSON string.
 *
 * @example Convert a `Uint8Array` to JSON
 *
 * ```ts
 * import { toJSON } from "jsr:/@kitsonk/kv-toolbox/blob";
 *
 * const u8 = new Uint8Array();
 * const json = JSON.stringify(toJSON(u8));
 * ```
 */
export async function toJSON(blob: File): Promise<BlobFileJSON>;
/**
 * Convert a typed array, array buffer, {@linkcode Blob} or {@linkcode File}
 * into a form that can be converted into a JSON string.
 *
 * @example Convert a `Uint8Array` to JSON
 *
 * ```ts
 * import { toJSON } from "jsr:/@kitsonk/kv-toolbox/blob";
 *
 * const u8 = new Uint8Array();
 * const json = JSON.stringify(toJSON(u8));
 * ```
 */
export async function toJSON(blob: Blob): Promise<BlobBlobJSON>;
/**
 * Convert a typed array, array buffer, {@linkcode Blob} or {@linkcode File}
 * into a form that can be converted into a JSON string.
 *
 * @example Convert a `Uint8Array` to JSON
 *
 * ```ts
 * import { toJSON } from "jsr:/@kitsonk/kv-toolbox/blob";
 *
 * const u8 = new Uint8Array();
 * const json = JSON.stringify(toJSON(u8));
 * ```
 */
export async function toJSON(blob: ArrayBufferLike): Promise<BlobBufferJSON>;
export async function toJSON(
  blob: ArrayBufferLike | Blob | File,
): Promise<BlobJSON> {
  new Uint8Array();
  if (blob instanceof File) {
    return {
      meta: {
        kind: "file",
        type: blob.type,
        lastModified: blob.lastModified,
        name: blob.name,
      },
      parts: toParts(await blob.arrayBuffer()),
    };
  }
  if (blob instanceof Blob) {
    return {
      meta: { kind: "blob", type: blob.type },
      parts: toParts(await blob.arrayBuffer()),
    };
  }
  return { meta: { kind: "buffer" }, parts: toParts(blob) };
}

/**
 * Convert a previously encoded object into an instance of {@linkcode File}.
 *
 * @example Convert some JSON to a File
 *
 * ```ts
 * import { toValue } from "jsr:/@kitsonk/kv-toolbox/blob";
 *
 * const file = toValue({
 *   meta: {
 *     type: "file",
 *     lastModified: 1711349710546,
 *     name: "test.bin",
 *     type: "application/octet-stream",
 *   },
 *   parts: ["AQID"],
 * });
 * ```
 */
export function toValue(json: BlobFileJSON): File;
/**
 * Convert a previously encoded object into an instance of {@linkcode Blob}.
 *
 * @example Convert some JSON to a File
 *
 * ```ts
 * import { toValue } from "jsr:/@kitsonk/kv-toolbox/blob";
 *
 * const blob = toValue({
 *   meta: {
 *     type: "blob",
 *     type: "application/octet-stream",
 *   },
 *   parts: ["AQID"],
 * });
 * ```
 */
export function toValue(json: BlobBlobJSON): Blob;
/**
 * Convert a previously encoded object into an instance of
 * {@linkcode Uint8Array}.
 *
 * @example Convert some JSON to a File
 *
 * ```ts
 * import { toValue } from "jsr:/@kitsonk/kv-toolbox/blob";
 *
 * const u8 = toValue({ parts: ["AQID"] });
 * ```
 */
export function toValue(json: BlobBufferJSON): Uint8Array;
/**
 * Convert a previously encoded object into an instance of a
 * {@linkcode Uint8Array}, {@linkcode Blob}, or {@linkcode File}.
 *
 * @example Convert some JSON to a File
 *
 * ```ts
 * import { toValue } from "jsr:/@kitsonk/kv-toolbox/blob";
 *
 * const u8 = toValue({ parts: ["AQID"] });
 * ```
 */
export function toValue(json: BlobJSON): Uint8Array | Blob | File;
export function toValue(json: BlobJSON): Uint8Array | Blob | File {
  const parts = json.parts.map(decodeBase64Url);
  if (json.meta.kind === "file") {
    return new File(parts, json.meta.name, {
      type: json.meta.type,
      lastModified: json.meta.lastModified,
    });
  }
  if (json.meta.kind === "blob") {
    return new Blob(parts, { type: json.meta.type });
  }
  return concat(parts);
}
