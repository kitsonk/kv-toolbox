/**
 * # KV Toolbox
 *
 * [![jsr.io/@kitsonk/kv-toolbox](https://jsr.io/badges/@kitsonk/kv-toolbox)](https://jsr.io/@kitsonk/kv-toolbox)
 * [![jsr.io/@kitsonk/kv-toolbox score](https://jsr.io/badges/@kitsonk/kv-toolbox/score)](https://jsr.io/@kitsonk/kv-toolbox)
 * [![kv-toolbox ci](https://github.com/kitsonk/kv-toolbox/workflows/ci/badge.svg)](https://github.com/kitsonk/kv-toolbox)
 *
 * Provides a toolbox for interacting with a Deno KV store, including the
 * ability to store values in an encrypted fashion. The main module exports a
 * {@linkcode ToolboxKv} class that augments the Deno KV store with additional
 * functionality. It also exports a {@linkcode CryptoToolboxKv} class that
 * augments the Deno KV store with additional functionality for encrypting and
 * decrypting blob values.
 *
 * Typically though, you would use the {@linkcode openKvToolbox} function to
 * create an instance of the toolbox. If you pass in an encryption key, it will
 * create an instance of the {@linkcode CryptoToolboxKv} class, otherwise it
 * will create an instance of the {@linkcode ToolboxKv} class.
 *
 * The {@linkcode generateKey} function is exported and can be used to generate
 * a random encryption key to use with the {@linkcode CryptoToolboxKv} class.
 *
 * ## Creating a toolbox
 *
 * To create an instance, you can use the {@linkcode openKvToolbox} function.
 * It is similar to {@linkcode Deno.openKv} but returns an instance of the
 * {@linkcode ToolboxKv} class:
 *
 * ```ts
 * import { openKvToolbox } from "@kitsonk/kv-toolbox";
 *
 * const kv = await openKvToolbox();
 * ```
 *
 * ## Creating an encrypted toolbox
 *
 * To create an instance of the {@linkcode CryptoToolboxKv} class, you can pass
 * in an encryption key:
 *
 * ```ts
 * import { openKvToolbox, generateKey } from "@kitsonk/kv-toolbox";
 *
 * const kv = await openKvToolbox({ encryptWith: generateKey() });
 * ```
 *
 * > [!NOTE]
 * > The encryption key should be kept secret and not shared with others. It
 * > also needs to be stored securely, as it is required to decrypt values. The
 * > above example generates a random encryption key, that will be lost when the
 * > script is run again.
 *
 * ## Additional Modules
 *
 * This is the default module for the entire toolbox, and the individual
 * capabilities of the toolbox are provided by additional exports of the
 * library:
 *
 * - [batched_atomic](https://jsr.io/@kitsonk/kv-toolbox/doc/batched_atomic) -
 *   Provides a way to perform atomic operations in batches while working around
 *   the limitations of Deno KV.
 * - [blob](https://jsr.io/@kitsonk/kv-toolbox/doc/blob) - Provides a way to
 *   store arbitrarily large binary data in Deno KV.
 * - [crypto](https://jsr.io/@kitsonk/kv-toolbox/doc/crypto) - Provides a way to
 *   encrypt and decrypt data in Deno KV.
 * - [keys](https://jsr.io/@kitsonk/kv-toolbox/doc/keys) - Provides convenience
 *   functions for working with keys in Deno KV.
 * - [query](https://jsr.io/@kitsonk/kv-toolbox/doc/query) - Provides a way to
 *   query/filter values in Deno KV.
 *
 * ## `@deno/kv-utils`
 *
 * Parts of `kv-toolbox` were contributed to the
 * [`@deno/kv-utils`](https://jsr.io/@deno/kv-utils) package, like specifically
 * the JSON serialization and the ability to import and export to NDJSON.
 *
 * @module
 */

import {
  exportEntries,
  type ExportEntriesOptions,
  type ExportEntriesOptionsBytes,
  type ExportEntriesOptionsResponse,
  type ExportEntriesOptionsString,
  importEntries,
  type ImportEntriesOptions,
  type ImportEntriesResult,
} from "@deno/kv-utils/import-export";

import { type BatchAtomicOptions, batchedAtomic, type BatchedAtomicOperation } from "./batched_atomic.ts";
import {
  type BlobJSON,
  type BlobKvListOptions,
  type BlobMeta,
  get,
  getAsBlob,
  getAsJSON,
  getAsResponse,
  getMeta,
  list,
  set,
} from "./blob.ts";
import { removeBlob } from "./blob_util.ts";
import { CryptoKv, type Encryptor } from "./crypto.ts";
import { keys, type KeyTree, tree, unique, uniqueCount, type UniqueCountElement } from "./keys.ts";
import { type Query, query, type QueryOptions } from "./query.ts";

export type { BatchAtomicOptions, BatchedAtomicOperation } from "./batched_atomic.ts";
export type { BlobJSON, BlobKvListOptions, BlobMeta } from "./blob.ts";
export { type Encryptor, generateKey } from "./crypto.ts";
export type { KeyTree, UniqueCountElement } from "./keys.ts";
export { Filter, PropertyPath, type Query } from "./query.ts";

/**
 * A toolbox for interacting with a Deno KV store.
 *
 * It matches the Deno KV API, but adds additional functionality like the
 * ability to manage arbitrary binary data, and the ability to batch import and
 * export data in NDJSON format as well as other convenience functions.
 */
export class KvToolbox implements Disposable {
  #kv: Deno.Kv;

  /**
   * The underlying {@linkcode Deno.Kv} instance.
   */
  get db(): Deno.Kv {
    return this.#kv;
  }

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
  }

  /**
   * Similar to {@linkcode Deno.Kv.prototype.atomic} but deals with the limits
   * of transactions imposed by Deno KV.
   *
   * When committing the transaction, the operation is broken up in batches and
   * each commit result from each batch is returned, unless there is a commit
   * error, where any pending batched operations will be abandoned and the last
   * item in the commit result array will be the error.
   */
  atomic(options?: BatchAtomicOptions): BatchedAtomicOperation {
    return batchedAtomic(this.#kv, options);
  }

  /**
   * Close the database connection. This will prevent any further operations
   * from being performed on the database, and interrupt any in-flight
   * operations immediately.
   */
  close(): void {
    return this.#kv.close();
  }

  /**
   * Get a symbol that represents the versionstamp of the current atomic
   * operation. This symbol can be used as the last part of a key in
   * `.set()`, both directly on the `Kv` object and on an `AtomicOperation`
   * object created from this `Kv` instance.
   */
  commitVersionstamp(): symbol {
    return this.#kv.commitVersionstamp();
  }

  /**
   * Resolves with an array of unique sub keys/prefixes for the provided prefix
   * along with the number of sub keys that match that prefix. The `count`
   * represents the number of sub keys, a value of `0` indicates that only the
   * exact key exists with no sub keys.
   *
   * This is useful when storing keys and values in a hierarchical/tree view,
   * where you are retrieving a list including counts and you want to know all
   * the unique _descendants_ of a key in order to be able to enumerate them.
   *
   * If you omit a `prefix`, all unique root keys are resolved.
   *
   * @example Getting a count of keys
   *
   * If you had the following keys stored in a datastore:
   *
   * ```
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * And you would get the following results when using `uniqueCount()`:
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * console.log(await kv.counts(["a"]));
   * // { key: ["a", "b"], count: 1 }
   * // { key: ["a", "d"], count: 2 }
   * await kv.close();
   * ```
   */
  counts(
    prefix?: Deno.KvKey,
    options?: Deno.KvListOptions,
  ): Promise<UniqueCountElement[]> {
    return uniqueCount(this.#kv, prefix, options);
  }

  /**
   * Delete the value for the given key from the database. If no value exists
   * for the key, this operation is a no-op.
   *
   * Optionally, the `blob` option can be set to `true` to delete a value that
   * has been set with `.setBlob()`.
   *
   * @example Deleting a value
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * await kv.delete(["foo"]);
   * ```
   *
   * @example Deleting a blob value
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * await kv.delete(["foo"], { blob: true });
   * ```
   */
  delete(key: Deno.KvKey, options: { blob?: boolean } = {}): Promise<void> {
    return options.blob ? removeBlob(this.#kv, key) : this.#kv.delete(key);
  }

  /**
   * Add a value into the database queue to be delivered to the queue
   * listener via {@linkcode Deno.Kv.listenQueue}.
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * await kv.enqueue("bar");
   * ```
   *
   * The `delay` option can be used to specify the delay (in milliseconds)
   * of the value delivery. The default delay is 0, which means immediate
   * delivery.
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * await kv.enqueue("bar", { delay: 60000 });
   * ```
   *
   * The `keysIfUndelivered` option can be used to specify the keys to
   * be set if the value is not successfully delivered to the queue
   * listener after several attempts. The values are set to the value of
   * the queued message.
   *
   * The `backoffSchedule` option can be used to specify the retry policy for
   * failed message delivery. Each element in the array represents the number of
   * milliseconds to wait before retrying the delivery. For example,
   * `[1000, 5000, 10000]` means that a failed delivery will be retried
   * at most 3 times, with 1 second, 5 seconds, and 10 seconds delay
   * between each retry.
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * await kv.enqueue("bar", {
   *   keysIfUndelivered: [["foo", "bar"]],
   *   backoffSchedule: [1000, 5000, 10000],
   * });
   * ```
   */
  enqueue(
    value: unknown,
    options?: {
      delay?: number;
      keysIfUndelivered?: Deno.KvKey[];
      backoffSchedule?: number[];
    },
  ): Promise<Deno.KvCommitResult> {
    return this.#kv.enqueue(value, options);
  }

  /**
   * Like {@linkcode Deno.Kv} `.list()` method, but returns a
   * {@linkcode Response} which will have a body that will be the exported
   * entries that match the selector.
   *
   * The response will contain the appropriate content type and the `filename`
   * option can be used to set the content disposition header so the client
   * understands a file is being downloaded.
   */
  export(
    selector: Deno.KvListSelector,
    options: ExportEntriesOptionsResponse,
  ): Response;
  /**
   * Like {@linkcode Deno.Kv} `.list()` method, but returns a
   * {@linkcode ReadableStream} where entries are converted to a JSON structure.
   *
   * This is ideal for streaming ndjson as part of a response.
   */
  export(
    selector: Deno.KvListSelector,
    options: ExportEntriesOptionsString,
  ): ReadableStream<string>;
  /**
   * Like {@linkcode Deno.Kv} `.list()` method, but returns a
   * {@linkcode ReadableStream} where entries are already converted to their raw
   * byte representation after being encoded as JSON.
   *
   * This is ideal for streaming ndjson as part of a response.
   */
  export(
    selector: Deno.KvListSelector,
    options?: ExportEntriesOptionsBytes,
  ): ReadableStream<Uint8Array>;
  export(
    selector: Deno.KvListSelector,
    options:
      | { close?: boolean; response: true; filename?: string }
      | (
        | ExportEntriesOptions
        | ExportEntriesOptionsBytes
        | ExportEntriesOptionsResponse
      )
        & { response?: boolean | undefined } = {},
  ): Response | ReadableStream<string | Uint8Array> {
    return exportEntries(this.#kv, selector, options);
  }

  /**
   * Retrieve the value and versionstamp for the given key from the database
   * in the form of a {@linkcode Deno.KvEntryMaybe}. If no value exists for
   * the key, the returned entry will have a `null` value and versionstamp.
   *
   * The `consistency` option can be used to specify the consistency level
   * for the read operation. The default consistency level is "strong". Some
   * use cases can benefit from using a weaker consistency level. For more
   * information on consistency levels, see the documentation for
   * {@linkcode Deno.KvConsistencyLevel}.
   *
   * @example Getting a value
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = await kv.get(["foo"]);
   * result.key; // ["foo"]
   * result.value; // "bar"
   * result.versionstamp; // "00000000000000010000"
   * ```
   */
  get<T = unknown>(
    key: Deno.KvKey,
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<Deno.KvEntryMaybe<T>> {
    return this.#kv.get(key, options);
  }

  /**
   * Retrieve a binary object from the store as a {@linkcode Response} that has
   * been previously {@linkcode set}. This will read the blob out of the KV
   * store as a stream and set information in the response based on what is
   * available from the source.
   *
   * If there are other headers required, they can be supplied in the options.
   *
   * Setting the `contentDisposition` to `true` will cause the function to
   * resolve with a {@linkcode Response} which has the `Content-Disposition` set
   * as an attachment with an appropriate file name. This is designed to send a
   * response that instructs the browser to attempt to download the requested
   * entry.
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
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   *
   * const server = Deno.serve((req) => {
   *   const key = new URL(req.url)
   *     .pathname
   *     .slice(1)
   *     .split("/");
   *   key[key.length - 1] = key[key.length - 1] || "index.html";
   *   return kv.getAsBlob(key, { response: true });
   * });
   *
   * server.finished.then(() => kv.close());
   * ```
   */
  getAsBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      response: true;
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
    },
  ): Promise<Response>;
  /**
   * Retrieve a binary object from the store as {@linkcode BlobJSON} that has
   * been previously {@linkcode set}.
   *
   * If there is no corresponding entry, the function will resolve to `null`.
   *
   * @example Getting a value
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const json = await kv.getAsBlob(["hello"], { json: true });
   * // do something with blob json
   * await kv.close();
   * ```
   */
  getAsBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      json: true;
    },
  ): Promise<BlobJSON | null>;
  /**
   * Retrieve a binary object from the store as a {@linkcode Blob},
   * {@linkcode File}, {@linkcode BlobJSON} or {@linkcode Response} that has
   * been previously {@linkcode set}.
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
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const blob = await kv.getAsBlob(["hello"]);
   * // do something with blob
   * await kv.close();
   * ```
   */
  getAsBlob(
    key: Deno.KvKey,
    options?: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      response?: boolean | undefined;
      json?: boolean | undefined;
    },
  ): Promise<Blob | File | null>;
  getAsBlob(
    key: Deno.KvKey,
    options?: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      response?: boolean | undefined;
      contentDisposition?: boolean | undefined;
      headers?: HeadersInit | undefined;
      notFoundBody?: BodyInit | undefined;
      notFoundHeaders?: HeadersInit | undefined;
      json?: boolean | undefined;
    },
  ): Promise<Blob | File | BlobJSON | Response | null> {
    return options?.response
      ? options?.json ? getAsJSON(this.#kv, key, options) : getAsResponse(this.#kv, key, options)
      : getAsBlob(this.#kv, key, options);
  }

  /**
   * Retrieve a binary object entry from the store with a given key that has
   * been set with `.setBlob()`.
   *
   * When setting the option `stream` to `true`, a {@linkcode Deno.KvEntryMaybe}
   * is resolved with a value of {@linkcode ReadableStream} to read the blob in
   * chunks of {@linkcode Uint8Array}.
   *
   * When setting the option `blob` to `true`, the promise resolves with a
   * {@linkcode Deno.KvEntryMaybe} with a value of {@linkcode Blob} or
   * {@linkcode File}. If the original file had been a {@linkcode File} or
   * {@linkcode Blob} it the resolved value will reflect that original value
   * including its properties. If it was not, it will be a {@linkcode Blob} with
   * a type of `""`.
   *
   * Otherwise the function resolves with a {@linkcode Deno.KvEntryMaybe} with a
   * value of {@linkcode Uint8Array}.
   *
   * @example
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const maybeEntry = await kv.getBlob(["hello"], { stream: true });
   * if (maybeEntry.value) {
   *   for await (const chunk of maybeEntry.value) {
   *     // do something with chunk
   *   }
   * }
   * await kv.close();
   * ```
   */
  getBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      stream: true;
    },
  ): Promise<Deno.KvEntryMaybe<ReadableStream<Uint8Array>>>;
  /**
   * Retrieve a binary object from the store with a given key that has been
   * set with `.setBlob()`.
   *
   * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
   * returned to read the blob in chunks of {@linkcode Uint8Array}.
   *
   * When setting the option `blob` to `true`, the promise resolves with a
   * {@linkcode Blob}, {@linkcode File}, or `null`. If the original file had
   * been a {@linkcode File} or {@linkcode Blob} it the resolved value will
   * reflect that original value including its properties. If it was not, it
   * will be a {@linkcode Blob} with a type of `""`.
   *
   * Otherwise the function resolves with a single {@linkcode Uint8Array} or
   * `null`.
   *
   * @example
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const blob = await kv.getBlob(["hello"], { blob: true });
   * // do something with blob
   * await kv.close();
   * ```
   */
  getBlob(
    key: Deno.KvKey,
    options: { consistency?: Deno.KvConsistencyLevel | undefined; blob: true },
  ): Promise<Deno.KvEntryMaybe<Blob | File>>;
  /**
   * Retrieve a binary object from the store with a given key that has been
   * set with `.setBlob()`.
   *
   * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
   * returned to read the blob in chunks of {@linkcode Uint8Array}
   *
   * When setting the option `blob` to `true`, the promise resolves with a
   * {@linkcode Blob}, {@linkcode File}, or `null`. If the original file had
   * been a {@linkcode File} or {@linkcode Blob} it the resolved value will
   * reflect that original value including its properties. If it was not, it
   * will be a {@linkcode Blob} with a type of `""`.
   *
   * Otherwise the function resolves with a single {@linkcode Uint8Array} or
   * `null`.
   *
   * @example
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const blob = await kv.getBlob(["hello"]);
   * // do something with ab
   * await kv.close();
   * ```
   */
  getBlob(
    key: Deno.KvKey,
    options?: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      blob?: boolean;
      stream?: boolean;
    },
  ): Promise<Deno.KvEntryMaybe<Uint8Array>>;
  getBlob(key: Deno.KvKey, options?: {
    consistency?: Deno.KvConsistencyLevel | undefined;
    blob?: boolean;
    stream?: boolean;
  }): Promise<
    Deno.KvEntryMaybe<ReadableStream<Uint8Array> | Uint8Array | File | Blob>
  > {
    return get(this.#kv, key, options);
  }

  /**
   * Retrieve multiple values and versionstamps from the database in the form
   * of an array of {@linkcode Deno.KvEntryMaybe} objects. The returned array
   * will have the same length as the `keys` array, and the entries will be in
   * the same order as the keys. If no value exists for a given key, the
   * returned entry will have a `null` value and versionstamp.
   *
   * The `consistency` option can be used to specify the consistency level
   * for the read operation. The default consistency level is "strong". Some
   * use cases can benefit from using a weaker consistency level. For more
   * information on consistency levels, see the documentation for
   * {@linkcode Deno.KvConsistencyLevel}.
   *
   * @example Getting multiple values
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = await kv.getMany([["foo"], ["baz"]]);
   * result[0].key; // ["foo"]
   * result[0].value; // "bar"
   * result[0].versionstamp; // "00000000000000010000"
   * result[1].key; // ["baz"]
   * result[1].value; // null
   * result[1].versionstamp; // null
   * ```
   */
  getMany<T extends readonly unknown[]>(
    keys: readonly [...{ [K in keyof T]: Deno.KvKey }],
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<{ [K in keyof T]: Deno.KvEntryMaybe<T[K]> }> {
    return this.#kv.getMany(keys, options);
  }

  /**
   * Retrieve a binary object's meta data from the store as a
   * {@linkcode Deno.KvEntryMaybe}.
   *
   * @example Getting meta data
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const maybeMeta = await kv.getMeta(["hello"]));
   * await kv.close();
   * ```
   */
  getMeta(
    key: Deno.KvKey,
    options?: { consistency?: Deno.KvConsistencyLevel | undefined },
  ): Promise<Deno.KvEntryMaybe<BlobMeta>> {
    return getMeta(this.#kv, key, options);
  }

  /**
   * Allows NDJSON to be imported in a target {@linkcode Deno.Kv}.
   *
   * The `data` can be in multiple forms, including {@linkcode ReadableStream},
   * {@linkcode Blob}, {@linkcode File}, {@linkcode ArrayBuffer}, typed array,
   * or string.
   */
  import(
    data:
      | ReadableStream<Uint8Array>
      | Blob
      | ArrayBufferView
      | ArrayBuffer
      | string,
    options?: ImportEntriesOptions,
  ): Promise<ImportEntriesResult> {
    return importEntries(this.#kv, data, options);
  }

  /**
   * Retrieve a list of keys in the database. The returned list is an
   * {@linkcode Deno.KvListIterator} which can be used to iterate over the
   * entries in the database.
   *
   * Each list operation must specify a selector which is used to specify the
   * range of keys to return. The selector can either be a prefix selector, or
   * a range selector:
   *
   * - A prefix selector selects all keys that start with the given prefix of
   *   key parts. For example, the selector `["users"]` will select all keys
   *   that start with the prefix `["users"]`, such as `["users", "alice"]`
   *   and `["users", "bob"]`. Note that you can not partially match a key
   *   part, so the selector `["users", "a"]` will not match the key
   *   `["users", "alice"]`. A prefix selector may specify a `start` key that
   *   is used to skip over keys that are lexicographically less than the
   *   start key.
   * - A range selector selects all keys that are lexicographically between
   *   the given start and end keys (including the start, and excluding the
   *   end). For example, the selector `["users", "a"], ["users", "n"]` will
   *   select all keys that start with the prefix `["users"]` and have a
   *   second key part that is lexicographically between `a` and `n`, such as
   *   `["users", "alice"]`, `["users", "bob"]`, and `["users", "mike"]`, but
   *   not `["users", "noa"]` or `["users", "zoe"]`.
   *
   * The `options` argument can be used to specify additional options for the
   * list operation. See the documentation for {@linkcode Deno.KvListOptions}
   * for more information.
   *
   * @example Iterating over a list of entries
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const entries = kv.list({ prefix: ["users"] });
   * for await (const entry of entries) {
   *   entry.key; // ["users", "alice"]
   *   entry.value; // { name: "Alice" }
   *   entry.versionstamp; // "00000000000000010000"
   * }
   * ```
   */
  list<T = unknown>(
    selector: Deno.KvListSelector,
    options?: Deno.KvListOptions,
  ): Deno.KvListIterator<T> {
    return this.#kv.list<T>(selector, options);
  }

  /**
   * Retrieve a list of keys in the database. The returned list is an
   * {@linkcode Deno.KvListIterator} which can be used to iterate over the blob
   * entries in the database, returning a {@linkcode ReadableStream} of
   * {@linkcode Uint8Array} chunks for each matching blob entry. Any other
   * values in the database will be ignored.
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
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const entries = kv.listBlob({ prefix: ["users"] }, { stream: true });
   * for await (const entry of entries) {
   *   entry.key; // ["users", "alice"]
   *   entry.value; // ReadableStream<Uint8Array>
   *   entry.versionstamp; // "00000000000000010000"
   * }
   * ```
   *
   * The `options` argument can be used to specify additional options for the
   * list operation. See the documentation for {@linkcode BlobKvListOptions}
   * for more information.
   */
  listBlob(
    selector: Deno.KvListSelector,
    options: BlobKvListOptions & { stream?: true },
  ): Deno.KvListIterator<Uint8Array>;
  /**
   * Retrieve a list of keys in the database. The returned list is an
   * {@linkcode Deno.KvListIterator} which can be used to iterate over the blob
   * entries in the database, returning a {@linkcode Blob} or {@linkcode File}
   * for each matching blob entry. Any other values in the database will be
   * ignored.
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
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const entries = kv.listBlob({ prefix: ["users"] }, { blob: true });
   * for await (const entry of entries) {
   *   entry.key; // ["users", "alice"]
   *   entry.value; // `Blob` or `File`
   *   entry.versionstamp; // "00000000000000010000"
   * }
   * ```
   *
   * The `options` argument can be used to specify additional options for the
   * list operation. See the documentation for {@linkcode BlobKvListOptions}
   * for more information.
   */
  listBlob(
    selector: Deno.KvListSelector,
    options: BlobKvListOptions & { blob?: true },
  ): Deno.KvListIterator<Blob | File>;
  /**
   * Retrieve a list of keys in the database. The returned list is an
   * {@linkcode Deno.KvListIterator} which can be used to iterate over the blob
   * entries in the database, returning an {@linkcode Uint8Array} for each matching
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
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const entries = kv.listBlob({ prefix: ["users"] }, { bytes: true });
   * for await (const entry of entries) {
   *   entry.key; // ["users", "alice"]
   *   entry.value; // Uint8Array
   *   entry.versionstamp; // "00000000000000010000"
   * }
   * ```
   *
   * The `options` argument can be used to specify additional options for the
   * list operation. See the documentation for {@linkcode BlobKvListOptions}
   * for more information.
   */
  listBlob(
    selector: Deno.KvListSelector,
    options: BlobKvListOptions & { bytes?: true },
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
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const entries = kv.listBlob({ prefix: ["users"] });
   * for await (const entry of entries) {
   *   entry.key; // ["users", "alice"]
   *   entry.value; // { kind: "buffer", size: 65536 }
   *   entry.versionstamp; // "00000000000000010000"
   * }
   * ```
   *
   * The `options` argument can be used to specify additional options for the
   * list operation. See the documentation for {@linkcode BlobKvListOptions}
   * for more information.
   */
  listBlob(
    selector: Deno.KvListSelector,
    options?: BlobKvListOptions,
  ): Deno.KvListIterator<BlobMeta>;
  listBlob(
    selector: Deno.KvListSelector,
    options?: BlobKvListOptions,
  ): Deno.KvListIterator<BlobMeta | Uint8Array | Blob | File | ReadableStream> {
    return list(this.#kv, selector, options);
  }

  /**
   * Listen for queue values to be delivered from the database queue, which
   * were enqueued with {@linkcode Deno.Kv.enqueue}. The provided handler
   * callback is invoked on every dequeued value. A failed callback
   * invocation is automatically retried multiple times until it succeeds
   * or until the maximum number of retries is reached.
   *
   * @example Listening for queue values
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * kv.listenQueue(async (msg: unknown) => {
   *   await kv.set(["foo"], msg);
   * });
   * ```
   */
  // deno-lint-ignore no-explicit-any
  listenQueue(handler: (value: any) => Promise<void> | void): Promise<void> {
    return this.#kv.listenQueue(handler);
  }

  /** Return an array of keys that match the `selector` in the target `kv`
   * store.
   *
   * @example Listing keys
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * console.log(await kv.keys({ prefix: ["hello"] }));
   * await kv.close();
   * ```
   */
  keys(
    selector: Deno.KvListSelector,
    options?: Deno.KvListOptions,
  ): Promise<Deno.KvKey[]> {
    return keys(this.#kv, selector, options);
  }

  /**
   * @param selector Query/filter entries from a {@linkcode Deno.Kv} instance.
   *
   * The query instance can be used to filter entries based on a set of
   * conditions. Then the filtered entries can be retrieved using the `.get()`
   * method, which returns an async iterator that will yield the entries that
   * match the conditions.
   *
   * At a base level a query works like the `Deno.Kv.prototype.list()` method, but
   * with the added ability to filter entries based on the query conditions.
   *
   * @example Querying blob values as `ReadableStream<Uint8Array>`
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = kv.query({ prefix: [] }, { stream: true })
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * kv.close();
   * ```
   */
  query(selector: Deno.KvListSelector, options: QueryOptions & { stream: true }): Query<ReadableStream<Uint8Array>>;
  /**
   * @param selector Query/filter entries from a {@linkcode Deno.Kv} instance.
   *
   * The query instance can be used to filter entries based on a set of
   * conditions. Then the filtered entries can be retrieved using the `.get()`
   * method, which returns an async iterator that will yield the entries that
   * match the conditions.
   *
   * At a base level a query works like the `Deno.Kv.prototype.list()` method, but
   * with the added ability to filter entries based on the query conditions.
   *
   * @example Querying blob values as `Blob` or `File`
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = kv.query({ prefix: [] }, { blob: true })
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * kv.close();
   * ```
   */
  query(selector: Deno.KvListSelector, options: QueryOptions & { blob: true }): Query<Blob | File>;
  /**
   * @param selector Query/filter entries from a {@linkcode Deno.Kv} instance.
   *
   * The query instance can be used to filter entries based on a set of
   * conditions. Then the filtered entries can be retrieved using the `.get()`
   * method, which returns an async iterator that will yield the entries that
   * match the conditions.
   *
   * At a base level a query works like the `Deno.Kv.prototype.list()` method, but
   * with the added ability to filter entries based on the query conditions.
   *
   * @example Querying blob values as `Uint8Array`
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = kv.query({ prefix: [] }, { bytes: true })
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * kv.close();
   * ```
   */
  query(selector: Deno.KvListSelector, options: QueryOptions & { bytes: true }): Query<Uint8Array>;
  /**
   * @param selector Query/filter entries from a {@linkcode Deno.Kv} instance.
   *
   * The query instance can be used to filter entries based on a set of
   * conditions. Then the filtered entries can be retrieved using the `.get()`
   * method, which returns an async iterator that will yield the entries that
   * match the conditions.
   *
   * At a base level a query works like the `Deno.Kv.prototype.list()` method, but
   * with the added ability to filter entries based on the query conditions.
   *
   * @example Querying blob values as `BlobMeta`
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = kv.query({ prefix: [] }, { meta: true })
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * kv.close();
   * ```
   */
  query(selector: Deno.KvListSelector, options: QueryOptions & { meta: true }): Query<BlobMeta>;
  /**
   * @param selector Query/filter entries from a {@linkcode Deno.Kv} instance.
   *
   * The query instance can be used to filter entries based on a set of
   * conditions. Then the filtered entries can be retrieved using the `.get()`
   * method, which returns an async iterator that will yield the entries that
   * match the conditions.
   *
   * At a base level a query works like the `Deno.Kv.prototype.list()` method, but
   * with the added ability to filter entries based on the query conditions.
   *
   * @example Filtering entries based on a property value
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = kv.query({ prefix: [] })
   *   .where("age", "<=", 10)
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * kv.close();
   * ```
   *
   * @example Filtering entries based on a property value using a `PropertyPath`
   *
   * ```ts
   * import { openKvToolbox, PropertyPath } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = kv.query({ prefix: [] })
   *   // matches { a: { b: { c: 1 } } }
   *   .where(new PropertyPath("a", "b", "c"), "==", 1)
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * kv.close();
   * ```
   *
   * @example Filtering entries based on an _or_ condition
   *
   * ```ts
   * import { openKvToolbox, Filter } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const result = kv.query({ prefix: [] })
   *   .where(Filter.or(
   *     Filter.where("age", "<", 10),
   *     Filter.where("age", ">", 20),
   *   ))
   *   .get();
   * for await (const entry of result) {
   *   console.log(entry);
   * }
   * kv.close();
   * ```
   */
  query<T = unknown>(selector: Deno.KvListSelector, options?: QueryOptions): Query<T>;
  query<T = unknown>(selector: Deno.KvListSelector, options: QueryOptions = {}): Query<T> {
    return query<T>(this.#kv, selector, options);
  }

  /**
   * Set the value for the given key in the database. If a value already
   * exists for the key, it will be overwritten.
   *
   * Optionally an `expireIn` option can be specified to set a time-to-live
   * (TTL) for the key. The TTL is specified in milliseconds, and the key will
   * be deleted from the database at earliest after the specified number of
   * milliseconds have elapsed. Once the specified duration has passed, the
   * key may still be visible for some additional time. If the `expireIn`
   * option is not specified, the key will not expire.
   *
   * @example Setting a value
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   * const kv = await openKvToolbox();
   * await kv.set(["foo"], "bar");
   * ```
   */
  set(
    key: Deno.KvKey,
    value: unknown,
    options?: { expireIn?: number },
  ): Promise<Deno.KvCommitResult> {
    return this.#kv.set(key, value, options);
  }

  /**
   * Set the blob value in with the provided key. The blob can be any array
   * buffer like structure, a byte {@linkcode ReadableStream}, or a
   * {@linkcode Blob}.
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
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const blob = new TextEncoder().encode("hello deno!");
   * await kv.setBlob(["hello"], blob);
   * await kv.close();
   * ```
   *
   * @example Setting a `Blob`
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const blob = new Blob(
   *   [new TextEncoder().encode("hello deno!")],
   *   { type: "text/plain" },
   * );
   * await kv.setBlob(["hello"], blob);
   * await kv.close();
   * ```
   */
  setBlob(
    key: Deno.KvKey,
    blob:
      | ArrayBufferLike
      | ArrayBufferView
      | ReadableStream<Uint8Array>
      | Blob
      | File,
    options?: { expireIn?: number },
  ): Promise<Deno.KvCommitResult> {
    return set(this.#kv, key, blob, options);
  }

  /**
   * Query a Deno KV store for keys and resolve with any matching keys
   * organized into a tree structure.
   *
   * The root of the tree will be either the root of Deno KV store or if a prefix
   * is supplied, keys that match the prefix. Each child node indicates if it
   * also has a value and any children of that node.
   *
   * @example Retrieving a tree
   *
   * If you had the following keys stored in a datastore:
   *
   * ```
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * And you would get the following results when using `tree()`:
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * console.log(await kv.tree(["a"]));
   * // {
   * //   prefix: ["a"],
   * //   children: [
   * //     {
   * //       part: "b",
   * //       hasValue: true,
   * //       children: [{ part: "c", hasValue: true }]
   * //     }, {
   * //       part: "d",
   * //       children: [
   * //         { part: "e", hasValue: true },
   * //         { part: "f", hasValue: true }
   * //       ]
   * //     }
   * //   ]
   * // }
   * await kv.close();
   * ```
   */
  tree(prefix?: Deno.KvKey, options?: Deno.KvListOptions): Promise<KeyTree> {
    return tree(this.#kv, prefix, options);
  }

  /**
   * Resolves with an array of unique sub keys/prefixes for the provided prefix.
   *
   * This is useful when storing keys and values in a hierarchical/tree view,
   * where you are retrieving a list and you want to know all the unique
   * _descendants_ of a key in order to be able to enumerate them.
   *
   * @example Retrieving unique keys
   *
   * The following keys stored in a datastore:
   *
   * ```
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * The following results when using `unique()`:
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * console.log(await kv.unique(["a"]));
   * // ["a", "b"]
   * // ["a", "d"]
   * await kv.close();
   * ```
   *
   * If you omit a `prefix`, all unique root keys are resolved.
   */
  unique(
    prefix?: Deno.KvKey | undefined,
    options?: Deno.KvListOptions | undefined,
  ): Promise<Deno.KvKey[]> {
    return unique(this.#kv, prefix, options);
  }

  /**
   * Watch for changes to the given keys in the database. The returned stream
   * is a {@linkcode ReadableStream} that emits a new value whenever any of
   * the watched keys change their versionstamp. The emitted value is an array
   * of {@linkcode Deno.KvEntryMaybe} objects, with the same length and order
   * as the `keys` array. If no value exists for a given key, the returned
   * entry will have a `null` value and versionstamp.
   *
   * The returned stream does not return every single intermediate state of
   * the watched keys, but rather only keeps you up to date with the latest
   * state of the keys. This means that if a key is modified multiple times
   * quickly, you may not receive a notification for every single change, but
   * rather only the latest state of the key.
   *
   * The `options` argument can be used to specify additional options for the
   * watch operation. The `raw` option can be used to specify whether a new
   * value should be emitted whenever a mutation occurs on any of the watched
   * keys (even if the value of the key does not change, such as deleting a
   * deleted key), or only when entries have observably changed in some way.
   * When `raw: true` is used, it is possible for the stream to occasionally
   * emit values even if no mutations have occurred on any of the watched
   * keys. The default value for this option is `false`.
   *
   * @example Watching for changes
   *
   * ```ts
   * import { openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   *
   * const stream = kv.watch([["foo"], ["bar"]]);
   * for await (const entries of stream) {
   *   entries[0].key; // ["foo"]
   *   entries[0].value; // "bar"
   *   entries[0].versionstamp; // "00000000000000010000"
   *   entries[1].key; // ["bar"]
   *   entries[1].value; // null
   *   entries[1].versionstamp; // null
   * }
   * ```
   */
  watch<T extends readonly unknown[]>(
    keys: readonly [...{ [K in keyof T]: Deno.KvKey }],
    options?: { raw?: boolean },
  ): ReadableStream<{ [K in keyof T]: Deno.KvEntryMaybe<T[K]> }> {
    return this.#kv.watch(keys, options);
  }

  [Symbol.dispose]() {
    const kv = this.#kv;
    // deno-lint-ignore no-explicit-any
    (this.#kv as any) = undefined;
    kv[Symbol.dispose]();
  }
}

/**
 * A {@linkcode ToolboxKv} that allows for encryption and decryption of blob
 * values that are stored in the underlying {@linkcode Deno.Kv}.
 *
 * By default, the `encrypted` option on blob methods will be true, attempting
 * to use the `encryptWith` option to encrypt or decrypt the value. This can be
 * bypassed by setting the `encrypted` option to `false` explicitly.
 */
export class CryptoKvToolbox extends KvToolbox {
  #cryptoKv: CryptoKv;
  constructor(kv: Deno.Kv, encryptWith: string | Uint8Array | Encryptor) {
    super(kv);
    this.#cryptoKv = new CryptoKv(kv, encryptWith);
  }

  /**
   * Retrieve a binary object entry from the store with a given key that has
   * been set with `.setBlob()`.
   *
   * When setting the option `stream` to `true`, a {@linkcode Deno.KvEntryMaybe}
   * is resolved with a value of {@linkcode ReadableStream} to read the blob in
   * chunks of {@linkcode Uint8Array}.
   *
   * When setting the option `blob` to `true`, the promise resolves with a
   * {@linkcode Deno.KvEntryMaybe} with a value of {@linkcode Blob} or
   * {@linkcode File}. If the original file had been a {@linkcode File} or
   * {@linkcode Blob} it the resolved value will reflect that original value
   * including its properties. If it was not, it will be a {@linkcode Blob} with
   * a type of `""`.
   *
   * Otherwise the function resolves with a {@linkcode Deno.KvEntryMaybe} with a
   * value of {@linkcode Uint8Array}.
   *
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the decryption.
   *
   * @example
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const maybeEntry = await kv.getBlob(["hello"], { stream: true });
   * if (maybeEntry.value) {
   *   for await (const chunk of maybeEntry.value) {
   *     // do something with chunk
   *   }
   * }
   * await kv.close();
   * ```
   */
  override getBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
      stream: true;
    },
  ): Promise<Deno.KvEntryMaybe<ReadableStream<Uint8Array>>>;
  /**
   * Retrieve a binary object from the store with a given key that has been
   * set with `.setBlob()`.
   *
   * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
   * returned to read the blob in chunks of {@linkcode Uint8Array}.
   *
   * When setting the option `blob` to `true`, the promise resolves with a
   * {@linkcode Blob}, {@linkcode File}, or `null`. If the original file had
   * been a {@linkcode File} or {@linkcode Blob} it the resolved value will
   * reflect that original value including its properties. If it was not, it
   * will be a {@linkcode Blob} with a type of `""`.
   *
   * Otherwise the function resolves with a single {@linkcode Uint8Array} or
   * `null`.
   *
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the decryption.
   *
   * @example Retrieving an encrypted blob
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const blob = await kv.getBlob(["hello"], { blob: true });
   * // do something with blob
   * await kv.close();
   * ```
   */
  override getBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
      blob: true;
    },
  ): Promise<Deno.KvEntryMaybe<Blob | File>>;
  /**
   * Retrieve a binary object from the store with a given key that has been
   * set with `.setBlob()`.
   *
   * When setting the option `stream` to `true`, a {@linkcode ReadableStream} is
   * returned to read the blob in chunks of {@linkcode Uint8Array}.
   *
   * When setting the option `blob` to `true`, the promise resolves with a
   * {@linkcode Blob}, {@linkcode File}, or `null`. If the original file had
   * been a {@linkcode File} or {@linkcode Blob} it the resolved value will
   * reflect that original value including its properties. If it was not, it
   * will be a {@linkcode Blob} with a type of `""`.
   *
   * Otherwise the function resolves with a single {@linkcode Uint8Array} or
   * `null`.
   *
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the decryption.
   *
   * @example Retrieving an encrypted blob
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const blob = await kv.getBlob(["hello"]);
   * // do something with ab
   * await kv.close();
   * ```
   */
  override getBlob(
    key: Deno.KvKey,
    options?: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
      blob?: boolean | undefined;
      stream?: boolean | undefined;
    } | undefined,
  ): Promise<Deno.KvEntryMaybe<Uint8Array>>;
  override getBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
      blob?: boolean | undefined;
      stream?: boolean | undefined;
    } | undefined = {},
  ): Promise<
    Deno.KvEntryMaybe<Uint8Array | ReadableStream<Uint8Array> | Blob | File>
  > {
    if (options.encrypted !== false && options.stream) {
      throw new TypeError("Encrypted blobs cannot be retrieved as streams.");
    }
    return options.encrypted === false ? super.getBlob(key, options) : this.#cryptoKv.getBlob(key, options);
  }

  /**
   * Retrieve a binary object from the store as a {@linkcode Response} that has
   * been previously {@linkcode set}. This will read the blob out of the KV
   * store as a stream and set information in the response based on what is
   * available from the source.
   *
   * > [!WARNING]
   * > Encrypted blobs cannot be retrieved as responses. The `encrypted` option
   * > must be set to `false` to retrieve a blob as a response.
   *
   * If there are other headers required, they can be supplied in the options.
   *
   * Setting the `contentDisposition` to `true` will cause the function to
   * resolve with a {@linkcode Response} which has the `Content-Disposition` set
   * as an attachment with an appropriate file name. This is designed to send a
   * response that instructs the browser to attempt to download the requested
   * entry.
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
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   *
   * const server = Deno.serve((req) => {
   *   const key = new URL(req.url)
   *     .pathname
   *     .slice(1)
   *     .split("/");
   *   key[key.length - 1] = key[key.length - 1] || "index.html";
   *   return kv.getAsBlob(key, { response: true, encrypted: false });
   * });
   *
   * server.finished.then(() => kv.close());
   * ```
   */
  override getAsBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel;
      encrypted: false;
      response: true;
      contentDisposition?: boolean | undefined;
      headers?: HeadersInit | undefined;
      notFoundBody?: BodyInit | undefined;
      notFoundHeaders?: HeadersInit | undefined;
    },
  ): Promise<Response>;
  /**
   * Retrieve a binary object from the store as a {@linkcode BlobJSON}.
   *
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the decryption.
   *
   * @example Retrieving an encrypted blob as JSON
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const value = await kv.getAsBlob(["hello"], { json: true });
   * if (value) {
   *   // do something with value
   * }
   * kv.close();
   * ```
   */
  override getAsBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel;
      encrypted?: boolean | undefined;
      json: true;
    },
  ): Promise<BlobJSON | null>;
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
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the decryption.
   *
   * @example Retrieving an encrypted blob
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const value = await kv.getAsBlob(["hello"]);
   * if (value) {
   *   // do something with value
   * }
   * kv.close();
   * ```
   */
  override getAsBlob(
    key: Deno.KvKey,
    options?: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
      json?: boolean | undefined;
    },
  ): Promise<Blob | File | null>;
  override getAsBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
      response?: boolean | undefined;
      contentDisposition?: boolean | undefined;
      headers?: HeadersInit | undefined;
      notFoundBody?: BodyInit | undefined;
      notFoundHeaders?: HeadersInit | undefined;
      json?: boolean | undefined;
    } = {},
  ): Promise<Blob | File | BlobJSON | Response | null> {
    if (options.response && options.encrypted !== false) {
      throw new TypeError("Encrypted blobs cannot be retrieved as responses.");
    }
    return options.encrypted === false
      ? super.getAsBlob(key, options)
      : options.json
      ? this.#cryptoKv.getAsJSON(key, options)
      : this.#cryptoKv.getAsBlob(key, options);
  }

  /**
   * Retrieve the meta data associated with a blob value for the provided key.
   * If the entry is not is not present or not a blob `null` will be resolved as
   * the value.
   *
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the decryption.
   *
   * @example
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const meta = await kv.getMeta(["hello"]);
   * if (meta.value) {
   *   // do something with meta
   * }
   * kv.close();
   * ```
   */
  override getMeta(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
    } | undefined = {},
  ): Promise<Deno.KvEntryMaybe<BlobMeta>> {
    return options.encrypted === false ? super.getMeta(key, options) : this.#cryptoKv.getBlobMeta(key, options);
  }

  /**
   * Create or update a blob entry with the option to encrypt the value. The
   * method stores the value in chunks within the store ensuring no individual
   * chunk exceeds the value size limitations imposed by Deno KV.
   *
   * The value can be an {@linkcode ArrayBuffer}, typed array,
   * {@linkcode DataView}, {@linkcode Blob}, or {@linkcode File}.
   *
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the encryption.
   *
   * Optionally an `expireIn` option can be specified to set a time-to-live
   * (TTL) for the key. The TTL is specified in milliseconds, and the key will
   * be deleted from the database at earliest after the specified number of
   * milliseconds have elapsed. Once the specified duration has passed, the
   * key may still be visible for some additional time. If the `expireIn`
   * option is not specified, the key will not expire.
   *
   * @example Storing an encrypted blob
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const res = await kv.setBlob(
   *   ["hello"],
   *   globalThis.crypto.getRandomValues(new Uint8Array(65_536)),
   * );
   * if (res.ok) {
   *   // the commit was successful
   * }
   * kv.close();
   * ```
   */
  override setBlob(
    key: Deno.KvKey,
    blob:
      | ReadableStream<Uint8Array>
      | Blob
      | ArrayBufferView
      | ArrayBufferLike
      | File,
    options:
      | { expireIn?: number | undefined; encrypted?: boolean | undefined }
      | undefined = {},
  ): Promise<Deno.KvCommitResult> {
    if (options.encrypted !== false && blob instanceof ReadableStream) {
      throw new TypeError("Encrypted blobs cannot be retrieved as streams.");
    }
    return options.encrypted === false
      ? super.setBlob(key, blob, options)
      : this.#cryptoKv.setBlob(key, blob as ArrayBufferLike, options);
  }

  /**
   * Listing encrypted blob is not currently supported.
   */
  override listBlob(
    selector: Deno.KvListSelector,
    options: BlobKvListOptions & {
      stream?: true;
      encrypted?: boolean | undefined;
    },
  ): Deno.KvListIterator<Uint8Array>;
  /**
   * Listing encrypted blob is not currently supported.
   */
  override listBlob(
    selector: Deno.KvListSelector,
    options: BlobKvListOptions & {
      blob?: true;
      encrypted?: boolean | undefined;
    },
  ): Deno.KvListIterator<Blob | File>;
  /**
   * Listing encrypted blob is not currently supported.
   */
  override listBlob(
    selector: Deno.KvListSelector,
    options: BlobKvListOptions & {
      bytes?: true;
      encrypted?: boolean | undefined;
    },
  ): Deno.KvListIterator<Uint8Array>;
  /**
   * Listing encrypted blob is not currently supported.
   */
  override listBlob(
    selector: Deno.KvListSelector,
    options?: BlobKvListOptions & { encrypted?: boolean | undefined },
  ): Deno.KvListIterator<BlobMeta>;
  override listBlob(
    selector: Deno.KvListSelector,
    options: BlobKvListOptions & { encrypted?: boolean | undefined } = {},
  ): Deno.KvListIterator<BlobMeta | Uint8Array | Blob | File | ReadableStream> {
    if (options.encrypted !== false) {
      throw new TypeError(
        "Listing encrypted blobs is not currently supported.",
      );
    }
    return super.listBlob(selector, options);
  }

  override [Symbol.dispose]() {
    // deno-lint-ignore no-explicit-any
    (this.#cryptoKv as any) = undefined;
    super[Symbol.dispose]();
  }
}

/**
 * Open a {@linkcode ToolboxKv} or {@linkcode CryptoToolboxKv} instance for
 * interacting with the Deno KV store.
 *
 * If the `encryptWith` option is provided, a {@linkcode CryptoToolboxKv} will
 * be returned, otherwise a {@linkcode ToolboxKv} will be returned.
 *
 * @example Opening a toolbox
 *
 * ```ts
 * import { openKvToolbox } from "@kitsonk/kv-toolbox";
 *
 * const kv = await openKvToolbox();
 * await kv.set(["hello"], "world");
 * await kv.close();
 * ```
 */
export function openKvToolbox(
  options: {
    path?: string | undefined;
    encryptWith: string | Uint8Array | Encryptor;
  },
): Promise<CryptoKvToolbox>;
/**
 * Open a {@linkcode ToolboxKv} or {@linkcode CryptoToolboxKv} instance for
 * interacting with the Deno KV store.
 *
 * If the `encryptWith` option is provided, a {@linkcode CryptoToolboxKv} will
 * be returned, otherwise a {@linkcode ToolboxKv} will be returned.
 *
 * @example Opening a toolbox
 *
 * ```ts
 * import { openKvToolbox } from "@kitsonk/kv-toolbox";
 *
 * const kv = await openKvToolbox();
 * await kv.set(["hello"], "world");
 * await kv.close();
 * ```
 */
export function openKvToolbox(
  options?: { path?: string | undefined; encryptWith?: undefined },
): Promise<KvToolbox>;
export async function openKvToolbox(
  options?: {
    path?: string | undefined;
    encryptWith?: string | Uint8Array | Encryptor | undefined;
  },
): Promise<KvToolbox | CryptoKvToolbox> {
  const kv = await Deno.openKv(options?.path);
  return options?.encryptWith ? new CryptoKvToolbox(kv, options.encryptWith) : new KvToolbox(kv);
}
