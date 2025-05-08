/**
 * A set of APIs for storing arbitrarily sized blobs in Deno KV. Currently Deno
 * KV has a limit of key values being 64k.
 *
 * The {@linkcode set} function breaks down a blob into chunks and manages
 * sub-keys to store the complete value, including preserving meta data
 * associated with {@linkcode Blob} and {@linkcode File} instances.
 *
 * The {@linkcode get}, {@linkcode getAsBlob}, {@linkcode getAsStream}, and
 * {@linkcode getAsJSON} functions reverse that process. {@linkcode get}
 * resolves with a standard {@linkcode Deno.KvEntryMaybe}, while the other
 * `get*` methods just return or resolve with the value.
 *
 * The {@linkcode getAsResponse} will resolve a blob entry as a
 * {@linkcode Response} using meta information to set appropriate headers. If
 * the entry is not found, the response will be set to a `404 Not Found`.
 *
 * {@linkcode remove} function will delete the key, sub-keys and values.
 *
 * {@linkcode getMeta} resolves with the meta data associated with a blob entry
 * stored in Deno KV. This information is useful for understanding the blob
 * value without having to read the blob out of the datastore. If the blob does
 * not exist `null` is resolved.
 *
 * {@linkcode toValue} and {@linkcode toJSON} are functions which allow
 * serializing blob like values to and from JSON.
 *
 * {@linkcode toBlob} is a convenience function to convert string values into
 * {@linkcode Blob}s for storage via {@linkcode set}.
 *
 * @example Basic usage
 *
 * ```ts
 * import { get, remove, set } from "@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const data = new TextEncoder().encode("hello deno!");
 * await set(kv, ["hello"], data);
 * const maybeAb = await get(kv, ["hello"]);
 * // do something with maybeAb
 * await remove(kv, ["hello"]);
 * await kv.close();
 * ```
 *
 * @example Setting and getting `File`s
 *
 * ```ts
 * import { getAsBlob, remove, set } from "@kitsonk/kv-toolbox/blob";
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

import { concat } from "@std/bytes/concat";
import { decodeBase64Url, encodeBase64Url } from "@std/encoding/base64url";
import { extension } from "@std/media-types/extension";

import { batchedAtomic } from "./batched_atomic.ts";
import {
  asBlob,
  asMeta,
  asStream,
  asUint8Array,
  BATCH_SIZE,
  BLOB_KEY,
  BLOB_META_KEY,
  BlobListIterator,
  type BlobMeta,
  CHUNK_SIZE,
  removeBlob,
  setBlob,
} from "./blob_util.ts";
import { keys } from "./keys.ts";

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

/**
 * Options which can be used when calling {@linkcode list}.
 */
export interface BlobKvListOptions extends Deno.KvListOptions {
  /**
   * If `true`, the value will be returned as a {@linkcode ReadableStream} of
   * {@linkcode Uint8Array} chunks.
   */
  stream?: boolean;
  /**
   * If `true`, the value will be returned as a {@linkcode Blob} or
   * {@linkcode File}.
   */
  blob?: boolean;
  /**
   * If `true`, the value will be returned as a {@linkcode Uint8Array}.
   */
  bytes?: boolean;
  /**
   * If `true`, the value will be returned as a {@linkcode BlobMeta} object.
   * This is the default.
   */
  meta?: boolean;
}

async function asJSON(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined },
): Promise<BlobJSON | null> {
  const prefix = [...key, BLOB_KEY];
  const prefixLength = prefix.length;
  const list = kv.list<Uint8Array>({ prefix }, {
    ...options,
    batchSize: BATCH_SIZE,
  });
  let found = false;
  const parts: Uint8Array[] = [];
  let i = 1;
  for await (const item of list) {
    if (
      item.value && item.key.length === prefixLength + 1 &&
      item.key[prefixLength] === i
    ) {
      i++;
      found = true;
      if (!(item.value instanceof Uint8Array)) {
        throw new TypeError("KV value is not a Uint8Array");
      }
      parts.push(item.value);
    } else {
      break;
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

function toParts(blob: ArrayBufferLike | ArrayBufferView): string[] {
  const buffer = ArrayBuffer.isView(blob)
    ? new Uint8Array(blob.buffer)
    : new Uint8Array(blob);
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
 * @example
 *
 * ```ts
 * import { remove } from "@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * await remove(kv, ["hello"]);
 * await kv.close();
 * ```
 */
export function remove(kv: Deno.Kv, key: Deno.KvKey): Promise<void> {
  return removeBlob(kv, key);
}

/** Retrieve a binary object entry from the store with a given key that has been
 * {@linkcode set}.
 *
 * When setting the option `stream` to `true`, a {@linkcode Deno.KvEntryMaybe}
 * is resolved with a value of {@linkcode ReadableStream} to read the blob in
 * chunks of {@linkcode Uint8Array}.
 *
 * When setting the option `blob` to `true`, the promise resolves with a
 * {@linkcode Deno.KvEntryMaybe} with a value of {@linkcode Blob} or
 * {@linkcode File}. If the original file had been a {@linkcode File} or
 * {@linkcode Blob} it the resolved value will reflect that original value
 * including its properties. If it was not, it will be a {@linkcode Blob} with a
 * type of `""`.
 *
 * Otherwise the function resolves with a {@linkcode Deno.KvEntryMaybe} with a
 * value of {@linkcode Uint8Array}.
 *
 * @example
 *
 * ```ts
 * import { get } from "@kitsonk/kv-toolbox/blob";
 * import { assert } from "@std/assert";
 *
 * const kv = await Deno.openKv();
 * const maybeEntry = await get(kv, ["hello"], { stream: true });
 * assert(maybeEntry.value);
 * for await (const chunk of maybeEntry.value) {
 *   // do something with chunk
 * }
 * await kv.close();
 * ```
 */
export function get(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined; stream: true },
): Promise<Deno.KvEntryMaybe<ReadableStream<Uint8Array>>>;
/** Retrieve a binary object from the store with a given key that has been
 * {@linkcode set}.
 *
 * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
 * returned to read the blob in chunks of {@linkcode Uint8Array}.
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
 * import { get } from "@kitsonk/kv-toolbox/blob";
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
): Promise<Deno.KvEntryMaybe<Blob | File>>;
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
 * import { get } from "@kitsonk/kv-toolbox/blob";
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
): Promise<Deno.KvEntryMaybe<Uint8Array>>;
export async function get(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: {
    consistency?: Deno.KvConsistencyLevel | undefined;
    blob?: boolean;
    stream?: boolean;
  } = {},
): Promise<
  Deno.KvEntryMaybe<ReadableStream<Uint8Array> | Uint8Array | File | Blob>
> {
  const meta = await asMeta(kv, key, options);
  const value = options.stream
    ? asStream(kv, key, options)
    : options.blob
    ? await asBlob(kv, key, options, meta)
    : await asUint8Array(kv, key, options);
  if (!value || !meta.value) {
    return { key: [...key], value: null, versionstamp: null };
  }
  return { key: [...key], value, versionstamp: meta.versionstamp };
}

/**
 * Retrieve a list of keys in the database. The returned list is an
 * {@linkcode Deno.KvListIterator} which can be used to iterate over the blob
 * entries in the database, returning a {@linkcode ReadableStream} for each
 * matching blob entry. Any other values in the database will be ignored.
 *
 * Each list operation must specify a selector which is used to specify the
 * range of keys to return. The selector can either be a prefix selector, or a
 * range selector:
 *
 * - A prefix selector selects all keys that start with the given prefix of key
 *   parts. For example, the selector `["users"]` will select all keys that
 *   start with the prefix `["users"]`, such as `["users", "alice"]` and
 *   `["users", "bob"]`. Note that you can not partially match a key part, so
 *   the selector `["users", "a"]` will not match the key `["users", "alice"]`.
 *   A prefix selector may specify a `start` key that is used to skip over keys
 *   that are lexicographically less than the start key.
 * - A range selector selects all keys that are lexicographically between the
 *   given start and end keys (including the start, and excluding the end). For
 *   example, the selector `["users", "a"], ["users", "n"]` will select all keys
 *   that start with the prefix `["users"]` and have a second key part that is
 *   lexicographically between `a` and `n`, such as `["users", "alice"]`,
 *   `["users", "bob"]`, and `["users", "mike"]`, but not `["users", "noa"]` or
 *   `["users", "zoe"]`.
 *
 * ```ts
 * import { list } from "@kitsonk/kv-toolbox/blob";
 *
 * const db = await Deno.openKv();
 * const entries = list(db, { prefix: ["users"] });
 * for await (const entry of entries) {
 *   entry.key; // ["users", "alice"]
 *   entry.value; // Uint8Array()
 *   entry.versionstamp; // "00000000000000010000"
 * }
 * ```
 *
 * The `options` argument can be used to specify additional options for the
 * list operation. See the documentation for {@linkcode Deno.KvListOptions}
 * for more information.
 */
export function list(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options: BlobKvListOptions & { stream: true },
): Deno.KvListIterator<ReadableStream<Uint8Array>>;
/**
 * Retrieve a list of keys in the database. The returned list is an
 * {@linkcode Deno.KvListIterator} which can be used to iterate over the blob
 * entries in the database, returning a {@linkcode Blob} or {@linkcode File} for
 * each matching blob entry. Any other values in the database will be ignored.
 *
 * Each list operation must specify a selector which is used to specify the
 * range of keys to return. The selector can either be a prefix selector, or a
 * range selector:
 *
 * - A prefix selector selects all keys that start with the given prefix of key
 *   parts. For example, the selector `["users"]` will select all keys that
 *   start with the prefix `["users"]`, such as `["users", "alice"]` and
 *   `["users", "bob"]`. Note that you can not partially match a key part, so
 *   the selector `["users", "a"]` will not match the key `["users", "alice"]`.
 *   A prefix selector may specify a `start` key that is used to skip over keys
 *   that are lexicographically less than the start key.
 * - A range selector selects all keys that are lexicographically between the
 *   given start and end keys (including the start, and excluding the end). For
 *   example, the selector `["users", "a"], ["users", "n"]` will select all keys
 *   that start with the prefix `["users"]` and have a second key part that is
 *   lexicographically between `a` and `n`, such as `["users", "alice"]`,
 *   `["users", "bob"]`, and `["users", "mike"]`, but not `["users", "noa"]` or
 *   `["users", "zoe"]`.
 *
 * ```ts
 * import { list } from "@kitsonk/kv-toolbox/blob";
 *
 * const db = await Deno.openKv();
 * const entries = list(db, { prefix: ["users"] });
 * for await (const entry of entries) {
 *   entry.key; // ["users", "alice"]
 *   entry.value; // Blob or File
 *   entry.versionstamp; // "00000000000000010000"
 * }
 * ```
 *
 * The `options` argument can be used to specify additional options for the
 * list operation. See the documentation for {@linkcode Deno.KvListOptions}
 * for more information.
 */
export function list(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options: BlobKvListOptions & { blob: true },
): Deno.KvListIterator<Blob | File>;
/**
 * Retrieve a list of keys in the database. The returned list is an
 * {@linkcode Deno.KvListIterator} which can be used to iterate over the blob
 * entries in the database, returning a `Uint8Array` for each matching blob
 * entry. Any other values in the database will be ignored.
 *
 * Each list operation must specify a selector which is used to specify the
 * range of keys to return. The selector can either be a prefix selector, or a
 * range selector:
 *
 * - A prefix selector selects all keys that start with the given prefix of key
 *   parts. For example, the selector `["users"]` will select all keys that
 *   start with the prefix `["users"]`, such as `["users", "alice"]` and
 *   `["users", "bob"]`. Note that you can not partially match a key part, so
 *   the selector `["users", "a"]` will not match the key `["users", "alice"]`.
 *   A prefix selector may specify a `start` key that is used to skip over keys
 *   that are lexicographically less than the start key.
 * - A range selector selects all keys that are lexicographically between the
 *   given start and end keys (including the start, and excluding the end). For
 *   example, the selector `["users", "a"], ["users", "n"]` will select all keys
 *   that start with the prefix `["users"]` and have a second key part that is
 *   lexicographically between `a` and `n`, such as `["users", "alice"]`,
 *   `["users", "bob"]`, and `["users", "mike"]`, but not `["users", "noa"]` or
 *   `["users", "zoe"]`.
 *
 * ```ts
 * import { list } from "@kitsonk/kv-toolbox/blob";
 *
 * const db = await Deno.openKv();
 * const entries = list(db, { prefix: ["users"] });
 * for await (const entry of entries) {
 *   entry.key; // ["users", "alice"]
 *   entry.value; // Uint8Array()
 *   entry.versionstamp; // "00000000000000010000"
 * }
 * ```
 *
 * The `options` argument can be used to specify additional options for the
 * list operation. See the documentation for {@linkcode Deno.KvListOptions}
 * for more information.
 */
export function list(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options: BlobKvListOptions & { bytes: true },
): Deno.KvListIterator<Uint8Array>;
/**
 * Retrieve a list of keys in the database. The returned list is an
 * {@linkcode Deno.KvListIterator} which can be used to iterate over the blob
 * entries in the database, returning the meta information for each matching
 * blob entry. Any other values in the database will be ignored.
 *
 * Each list operation must specify a selector which is used to specify the
 * range of keys to return. The selector can either be a prefix selector, or a
 * range selector:
 *
 * - A prefix selector selects all keys that start with the given prefix of key
 *   parts. For example, the selector `["users"]` will select all keys that
 *   start with the prefix `["users"]`, such as `["users", "alice"]` and
 *   `["users", "bob"]`. Note that you can not partially match a key part, so
 *   the selector `["users", "a"]` will not match the key `["users", "alice"]`.
 *   A prefix selector may specify a `start` key that is used to skip over keys
 *   that are lexicographically less than the start key.
 * - A range selector selects all keys that are lexicographically between the
 *   given start and end keys (including the start, and excluding the end). For
 *   example, the selector `["users", "a"], ["users", "n"]` will select all keys
 *   that start with the prefix `["users"]` and have a second key part that is
 *   lexicographically between `a` and `n`, such as `["users", "alice"]`,
 *   `["users", "bob"]`, and `["users", "mike"]`, but not `["users", "noa"]` or
 *   `["users", "zoe"]`.
 *
 * ```ts
 * import { list } from "@kitsonk/kv-toolbox/blob";
 *
 * const db = await Deno.openKv();
 * const entries = list(db, { prefix: ["users"] });
 * for await (const entry of entries) {
 *   entry.key; // ["users", "alice"]
 *   entry.value; // { kind: "buffer", size: 65536 }
 *   entry.versionstamp; // "00000000000000010000"
 * }
 * ```
 *
 * The `options` argument can be used to specify additional options for the
 * list operation. See the documentation for {@linkcode Deno.KvListOptions}
 * for more information.
 */
export function list(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options?: BlobKvListOptions,
): Deno.KvListIterator<BlobMeta>;
export function list(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options: BlobKvListOptions = {},
): Deno.KvListIterator<
  BlobMeta | ReadableStream<Uint8Array> | Uint8Array | File | Blob
> {
  const { stream, blob, meta, bytes } = options;
  if ([stream, blob, bytes, meta].filter(Boolean).length > 1) {
    throw new TypeError("Cannot set multiple value types as once.");
  }
  const valueKind = stream
    ? "stream"
    : blob
    ? "blob"
    : bytes
    ? "bytes"
    : "meta";
  return new BlobListIterator(kv, selector, options, valueKind);
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
 * import { getAsBlob } from "@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 * const blob = await getAsBlob(kv, ["hello"]);
 * // do something with blob
 * await kv.close();
 * ```
 */
export async function getAsBlob(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
): Promise<Blob | File | null> {
  const maybeMeta = await asMeta(kv, key, options);
  return asBlob(kv, key, options, maybeMeta);
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
 * import { getAsJSON } from "@kitsonk/kv-toolbox/blob";
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
 * Retrieve a binary object's meta data from the store as a
 * {@linkcode Deno.KvEntryMaybe}.
 *
 * @example Getting meta data
 *
 * ```ts
 * import { getMeta } from "@kitsonk/kv-toolbox/blob";
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
): Promise<Deno.KvEntryMaybe<BlobMeta>> {
  return asMeta(kv, key, options);
}

/** Options which can be used when calling {@linkcode getAsResponse}. */
export interface GetAsResponseOptions {
  consistency?: Deno.KvConsistencyLevel | undefined;
  /**
   * Set an appropriate content disposition header on the response. This will
   * cause a browser to usually treat the response as a download.
   *
   * If a filename is available, it will be used, otherwise a filename and
   * extension derived from the key and content type.
   */
  contentDisposition?: boolean | undefined;
  /** Any headers init to be used in conjunction with creating the request. */
  headers?: HeadersInit | undefined;
  /** If the blob entry is not present, utilize this body when responding. This
   * defaults to `null`. */
  notFoundBody?: BodyInit | undefined;
  /** If the blob entry is not present, utilize this headers init when
   * responding. */
  notFoundHeaders?: HeadersInit | undefined;
}

/**
 * Retrieve a blob value as a {@linkcode Response} which is suitable for sending
 * as a response to an HTTP request. This will read the blob out of the KV store
 * as a stream and set information in the response based on what is available
 * from the source.
 *
 * If there are other headers required, they can be supplied in the options.
 *
 * Setting the `contentDisposition` to `true` will cause the function to resolve
 * with a {@linkcode Response} which has the `Content-Disposition` set as an
 * attachment with an appropriate file name. This is designed to send a response
 * that instructs the browser to attempt to download the requested entry.
 *
 * If the blob entry is not present, the response will be set to a
 * `404 Not Found` with a `null` body. The not found body and headers can be
 * set in the options.
 *
 * @example Serving static content from Deno KV
 *
 * Creates a simple web server where the content has already been set in the
 * Deno KV datastore as `Blob`s. This is a silly example just to show
 * functionality and would be terribly inefficient in production:
 *
 * ```ts
 * import { getAsResponse } from "@kitsonk/kv-toolbox/blob";
 *
 * const kv = await Deno.openKv();
 *
 * const server = Deno.serve((req) => {
 *   const key = new URL(req.url)
 *     .pathname
 *     .slice(1)
 *     .split("/");
 *   key[key.length - 1] = key[key.length - 1] || "index.html";
 *   return getAsResponse(kv, key);
 * });
 *
 * server.finished.then(() => kv.close());
 * ```
 */
export async function getAsResponse(
  kv: Deno.Kv,
  key: Deno.KvKey,
  options: GetAsResponseOptions = {},
): Promise<Response> {
  const maybeMeta = await asMeta(kv, key, options);
  if (!maybeMeta.value) {
    const { notFoundBody = null, notFoundHeaders: headers } = options;
    return new Response(notFoundBody, {
      status: 404,
      statusText: "Not Found",
      headers,
    });
  }
  const headers = new Headers(options.headers);
  const contentType =
    (maybeMeta.value.kind !== "buffer" && maybeMeta.value.type) ||
    "application/octet-stream";
  headers.set("content-type", contentType);
  if (maybeMeta.value.size) {
    headers.set("content-length", String(maybeMeta.value.size));
  }
  if (options.contentDisposition) {
    const filename = maybeMeta.value.kind === "file" && maybeMeta.value.name ||
      `${
        typeof key[key.length - 1] !== "object" &&
          String(key[key.length - 1]) || "file"
      }.${extension(contentType) ?? "bin"}`;
    headers.set("content-disposition", `attachment; filename="${filename}"`);
  }
  const body = asStream(kv, key, options);
  return new Response(body, {
    headers,
    status: 200,
    statusText: "OK",
  });
}

/**
 * Retrieve a binary object from the store as a byte {@linkcode ReadableStream}.
 *
 * If there is no corresponding entry, the stream will provide no chunks.
 *
 * @example Getting a value
 *
 * ```ts
 * import { getAsStream } from "@kitsonk/kv-toolbox/blob";
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
 * import { set } from "@kitsonk/kv-toolbox/blob";
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
 * import { set } from "@kitsonk/kv-toolbox/blob";
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
  blob:
    | ArrayBufferLike
    | ArrayBufferView
    | ReadableStream<Uint8Array>
    | Blob
    | File,
  options?: { expireIn?: number },
): Promise<Deno.KvCommitResult> {
  const items = await keys(kv, { prefix: [...key, BLOB_KEY] });
  let operation = batchedAtomic(kv);
  operation = await setBlob(operation, key, blob, items.length, options);
  const res = await operation.commit();
  if (!res[0].ok) {
    throw new Error("Unexpected error when setting blob.");
  }
  return res[0];
}

/**
 * A convenience function which converts a string value to a {@linkcode Blob}
 * which can be stored via {@link set}. The function optionally takes a `type`
 * which represents the media type of the string (e.g. `"text/plain"` or
 * `"text/html"`). `type` defaults to `"text/plain"`.
 *
 * @example Storing and retrieving a blob string
 *
 * ```ts
 * import { getAsBlob, set, toBlob } from "@kitsonk/kv-toolbox/blob";
 * import { assert } from "@std/assert";
 *
 * const kv = await Deno.openKv();
 * const blob = toBlob("some big string");
 * await set(kv, ["hello"], blob);
 * const value = await getAsBlob(kv, ["hello"]);
 * assert(value);
 * const str = await value.text();
 * await kv.close();
 * ```
 */
export function toBlob(value: string, type = "text/plain"): Blob {
  return new Blob([value], { type });
}

/**
 * Convert a typed array, array buffer, {@linkcode Blob} or {@linkcode File}
 * into a form that can be converted into a JSON string.
 *
 * @example Convert a `Uint8Array` to JSON
 *
 * ```ts
 * import { toJSON } from "@kitsonk/kv-toolbox/blob";
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
 * import { toJSON } from "@kitsonk/kv-toolbox/blob";
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
 * import { toJSON } from "@kitsonk/kv-toolbox/blob";
 *
 * const u8 = new Uint8Array();
 * const json = JSON.stringify(toJSON(u8));
 * ```
 */
export async function toJSON(
  blob: ArrayBufferLike | ArrayBufferView,
): Promise<BlobBufferJSON>;
export async function toJSON(
  blob: ArrayBufferLike | ArrayBufferView | Blob | File,
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
 * import { toValue } from "@kitsonk/kv-toolbox/blob";
 *
 * const file = toValue({
 *   meta: {
 *     kind: "file",
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
 * import { toValue } from "@kitsonk/kv-toolbox/blob";
 *
 * const blob = toValue({
 *   meta: {
 *     kind: "blob",
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
 * import { toValue } from "@kitsonk/kv-toolbox/blob";
 *
 * const u8 = toValue({
 *   meta: { kind: "buffer" },
 *   parts: ["AQID"],
 * });
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
 * import { toValue } from "@kitsonk/kv-toolbox/blob";
 *
 * const u8 = toValue({
 *   meta: { kind: "buffer" },
 *   parts: ["AQID"],
 * });
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
