/**
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
 * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
 * import { openKvToolbox, generateKey } from "jsr:@kitsonk/kv-toolbox";
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
 * - [batched_atomic](./doc/batched_atomic/~) - Provides a way to perform
 *   atomic operations in batches while working around the limitations of
 *   Deno KV.
 * - [blob](./doc/blob/~) - Provides a way to store arbitrarily large binary
 *   data in Deno KV.
 * - [crypto](./doc/crypto/~) - Provides a way to encrypt and decrypt data in
 *   Deno KV.
 * - [json](./doc/json/~) - Provides utilities for handling Deno KV entries,
 *   keys, and values as structures which can be serialized and deserialized to
 *   JSON.
 * - [keys](./doc/keys/~) - Provides convenience functions for working with
 *   keys in Deno KV.
 * - [ndjson](./doc/ndjson/~) - Utilities for handling NDJSON which is a method
 *   for encoding JSON in a way that supports streaming, where each JSON entity
 *   is separated with a newline.
 *
 * @module
 */

import {
  type BatchAtomicOptions,
  batchedAtomic,
  type BatchedAtomicOperation,
} from "./batched_atomic.ts";
import { type BlobMeta, get, getAsBlob, getMeta, set } from "./blob.ts";
import { removeBlob } from "./blob_util.ts";
import { CryptoKv, type Encryptor } from "./crypto.ts";
import {
  keys,
  type KeyTree,
  tree,
  unique,
  uniqueCount,
  type UniqueCountElement,
} from "./keys.ts";
import {
  exportEntries,
  type ExportEntriesOptionsBytes,
  type ExportEntriesOptionsJSON,
  exportToResponse,
  importEntries,
  type ImportEntriesOptions,
  type ImportEntriesResult,
} from "./ndjson.ts";

export { generateKey } from "./crypto.ts";

interface ExportEntriesOptionsResponse {
  close?: boolean;
  response: true;
}

/**
 * A toolbox for interacting with a Deno KV store.
 *
 * It matches the Deno KV API, but adds additional functionality like the
 * ability to manage arbitrary binary data, and the ability to batch import and
 * export data in NDJSON format as well as other convenience functions.
 */
export class KvToolbox implements Disposable {
  #kv: Deno.Kv;

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
   * ```ts
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * And you would get the following results when using `uniqueCount()`:
   *
   * ```ts
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * console.log(await kv.count(["a"]));
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * await kv.delete(["foo"]);
   * ```
   *
   * @example Deleting a blob value
   *
   * ```ts
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
    options: ExportEntriesOptionsJSON,
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
      | ExportEntriesOptionsResponse
      | ExportEntriesOptionsJSON
      | ExportEntriesOptionsBytes = {},
  ): Response | ReadableStream<string | Uint8Array> {
    return (options as ExportEntriesOptionsResponse).response
      ? exportToResponse(this.#kv, selector, options)
      : exportEntries(this.#kv, selector, options);
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const blob = await kv.getAsBlob(["hello"]);
   * // do something with blob
   * await kv.close();
   * ```
   */
  getAsBlob(
    key: Deno.KvKey,
    options?: { consistency?: Deno.KvConsistencyLevel | undefined },
  ): Promise<Blob | File | null> {
    return getAsBlob(this.#kv, key, options);
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox();
   * const stream = await kv.getBlob(["hello"], { stream: true });
   * for await (const chunk of stream) {
   *   // do something with chunk
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * Listen for queue values to be delivered from the database queue, which
   * were enqueued with {@linkcode Deno.Kv.enqueue}. The provided handler
   * callback is invoked on every dequeued value. A failed callback
   * invocation is automatically retried multiple times until it succeeds
   * or until the maximum number of retries is reached.
   *
   * @example Listening for queue values
   *
   * ```ts
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * ```ts
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * And you would get the following results when using `tree()`:
   *
   * ```ts
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * ```ts
   * ["a", "b"]
   * ["a", "b", "c"]
   * ["a", "d", "e"]
   * ["a", "d", "f"]
   * ```
   *
   * The following results when using `unique()`:
   *
   * ```ts
   * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
   * import { generateKey, openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const stream = await kv.getBlob(["hello"], { stream: true });
   * for await (const chunk of stream) {
   *   // do something with chunk
   * }
   * await kv.close();
   * ```
   */
  getBlob(
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
   * import { generateKey, openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const blob = await kv.getBlob(["hello"], { blob: true });
   * // do something with blob
   * await kv.close();
   * ```
   */
  getBlob(
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
   * import { generateKey, openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const blob = await kv.getBlob(["hello"]);
   * // do something with ab
   * await kv.close();
   * ```
   */
  getBlob(
    key: Deno.KvKey,
    options?: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
      blob?: boolean | undefined;
      stream?: boolean | undefined;
    } | undefined,
  ): Promise<Deno.KvEntryMaybe<Uint8Array>>;
  getBlob(
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
    return options.encrypted === false
      ? super.getBlob(key, options)
      : this.#cryptoKv.getBlob(key, options);
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
   * By default, the `encrypted` option is true and the `encryptWith` will be
   * used to decrypt the value. If the value is not encrypted, then the method
   * will return `null`. Explicitly setting the `encrypted` option to `false`
   * will bypass the decryption.
   *
   * @example Retrieving an encrypted blob
   *
   * ```ts
   * import { generateKey, openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const value = await kv.getAsBlob(["hello"]);
   * if (value) {
   *   // do something with value
   * }
   * kv.close();
   * ```
   */
  getAsBlob(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel;
      encrypted?: boolean;
    } = {},
  ): Promise<Blob | File | null> {
    return options.encrypted === false
      ? super.getAsBlob(key, options)
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
   * import { generateKey, openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
   *
   * const kv = await openKvToolbox({ encryptWith: generateKey() });
   * const meta = await kv.getMeta(["hello"]);
   * if (meta.value) {
   *   // do something with meta
   * }
   * kv.close();
   * ```
   */
  getMeta(
    key: Deno.KvKey,
    options: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      encrypted?: boolean | undefined;
    } | undefined = {},
  ): Promise<Deno.KvEntryMaybe<BlobMeta>> {
    return options.encrypted === false
      ? super.getMeta(key, options)
      : this.#cryptoKv.getBlobMeta(key, options);
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
   * import { generateKey, openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
  setBlob(
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

  [Symbol.dispose]() {
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
 * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
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
 * import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";
 *
 * const kv = await openKvToolbox();
 * await kv.set(["hello"], "world");
 * await kv.close();
 * ```
 */
export function openKvToolbox(
  options: { path?: string | undefined; encryptWith?: undefined },
): Promise<KvToolbox>;
export async function openKvToolbox(
  options?: {
    path?: string | undefined;
    encryptWith?: string | Uint8Array | Encryptor | undefined;
  },
): Promise<KvToolbox | CryptoKvToolbox> {
  const kv = await Deno.openKv(options?.path);
  return options?.encryptWith
    ? new CryptoKvToolbox(kv, options.encryptWith)
    : new KvToolbox(kv);
}
