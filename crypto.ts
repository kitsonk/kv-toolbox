/**
 * APIs for dealing with encrypted Deno KV values.
 *
 * The {@linkcode openCryptoKv} function will resolve with an instance of
 * {@linkcode CryptoKv} which all allow setting, getting and deleting encrypted
 * blobs.
 *
 * {@linkcode generateKey} is a function which will provide a new random crypto
 * key which can be used with {@linkcode CryptoKv}.
 *
 * {@linkcode Encryptor} is an interface which provides two methods which can
 * be used to encrypt and decrypt values instead of an crypto key.
 *
 * @example
 *
 * ```ts
 * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
 *
 * const kv = await openCryptoKv(generateKey());
 * const res = await kv.setBlob(
 *   ["hello"],
 *   globalThis.crypto.getRandomValues(new Uint8Array(65_536)),
 * );
 * if (res.ok) {
 *   const maybeValue = await kv.getBlob(["hello"]);
 *   await kv.deleteBlob(["hello"]);
 * }
 * kv.close();
 * ```
 *
 * @module
 */

import { assert } from "@std/assert/assert";
import { decodeHex, encodeHex } from "@std/encoding/hex";
import { concat } from "@std/bytes/concat";

import { batchedAtomic } from "./batched_atomic.ts";
import { BLOB_KEY, type BlobJSON, type BlobMeta, toJSON } from "./blob.ts";
import {
  asMeta,
  asUint8Array,
  BATCH_SIZE,
  removeBlob,
  setBlob,
} from "./blob_util.ts";
import { keys } from "./keys.ts";

/** Valid data types that can be used when supplying an encryption key. */
export type Key = string | number[] | ArrayBuffer | ArrayBufferView;

/**
 * An interface to supply encryption and decryption capability to
 * {@linkcode CryptoKv}. This allows for the ability to utilize items like
 * key rings and other types of external encryption capabilities.
 */
export interface Encryptor {
  /**
   * Encrypt the provided message, returning or resolving with the encrypted
   * value.
   */
  encrypt(message: Uint8Array): Uint8Array | Promise<Uint8Array>;
  /**
   * Decrypt the provided message, returning or resolving with the decrypted
   * value.
   */
  decrypt(
    message: Uint8Array,
  ): Uint8Array | undefined | Promise<Uint8Array | undefined>;
}

function isEncryptor(value: unknown): value is Encryptor {
  return !!(typeof value === "object" && value !== null && "encrypt" in value &&
    typeof value.encrypt === "function" && "decrypt" in value &&
    typeof value.decrypt === "function");
}

function importKey(key: string | Uint8Array): Promise<CryptoKey> {
  const rawKey = typeof key === "string" ? decodeHex(key) : key;
  return crypto.subtle.importKey(
    "raw",
    rawKey,
    "AES-GCM",
    true,
    ["encrypt", "decrypt"],
  );
}

/**
 * Wraps a {@linkcode Deno.Kv} to allow encryption and decryption of the values
 * set within the store.
 *
 * `kv` is an instance of a {@linkcode Deno.Kv}.
 *
 * `encryptWith` can be a hex encoded string or {@linkcode Uint8Array} that is
 * 128, 192, or 256 bits in length, or an object which conforms to the
 * {@linkcode Encryptor} interface.
 *
 * When a key is provided for `encryptWith`, values will be encrypted with
 * [`AES-GCM`](https://developer.mozilla.org/en-US/docs/Web/API/SubtleCrypto/encrypt#aes-gcm).
 * Each time a value is set a new random initialization vector will be used and
 * will be included with the value to be used for decryption.
 *
 * @example
 *
 * ```ts
 * import { CryptoKv, generateKey } from "jsr:@kitsonk/kv-toolbox/crypto";
 *
 * const kv = await Deno.openKv();
 * const cryptoKv = new CryptoKv(kv, generateKey());
 * // Perform operations
 * cryptoKv.close();
 * ```
 */
export class CryptoKv {
  #cryptoKey?: CryptoKey;
  #kv: Deno.Kv;
  #key?: string | Uint8Array;
  #encryptor?: Encryptor;

  async #asBlob(
    key: Deno.KvKey,
    options: { consistency?: Deno.KvConsistencyLevel },
    meta: BlobMeta,
  ) {
    let iv: Uint8Array | undefined;

    const decrypt = this.#encryptor
      ? (chunk: Uint8Array) => this.#encryptor!.decrypt(chunk)
      : async (chunk: Uint8Array) => {
        if (!iv) {
          assert(chunk.byteLength >= 12);
          iv = chunk.slice(0, 12);
          chunk = chunk.slice(12);
        }
        const cryptoKey = this.#cryptoKey ??
          (this.#cryptoKey = await importKey(this.#key!));
        return globalThis.crypto.subtle.decrypt(
          { name: "AES-GCM", iv },
          cryptoKey,
          chunk,
        );
      };

    const prefix = [...key, BLOB_KEY];
    const prefixLength = prefix.length;
    const list = this.#kv.list<Uint8Array>(
      { prefix },
      { ...options, batchSize: BATCH_SIZE },
    );
    const parts: Uint8Array[] = [];
    let i = 1;
    for await (const item of list) {
      if (
        item.value && item.key.length === prefixLength + 1 &&
        item.key[prefixLength] === i
      ) {
        if (!(item.value instanceof Uint8Array)) {
          throw new TypeError("KV value is not a Uint8Array.");
        }
        i++;
        parts.push(item.value);
      } else {
        // encountered an unexpected value part, abort
        return null;
      }
    }
    const value = await decrypt(concat(parts));
    if (!value) {
      return null;
    }
    if (meta.kind === "file") {
      return new File([value], meta.name, {
        lastModified: meta.lastModified,
        type: meta.type,
      });
    }
    if (meta.kind === "blob") {
      return new Blob([value], { type: meta.type });
    }
    return new Blob([value]);
  }

  async #encrypt(
    blob:
      | ArrayBufferView
      | ArrayBufferLike
      | Blob
      | File,
  ) {
    if (this.#encryptor) {
      if (ArrayBuffer.isView(blob)) {
        return this.#encryptor.encrypt(new Uint8Array(blob.buffer));
      } else if (
        blob instanceof ArrayBuffer || blob instanceof SharedArrayBuffer
      ) {
        return this.#encryptor.encrypt(new Uint8Array(blob));
      } else if (blob instanceof Blob) {
        const buffer = await this.#encryptor.encrypt(
          new Uint8Array(await blob.arrayBuffer()),
        );
        if (blob instanceof File) {
          const { type, name, lastModified } = blob;
          return new File([buffer], name, { type, lastModified });
        }
        const { type } = blob;
        return new Blob([buffer], { type });
      }
      throw TypeError(
        "Blobs must be an ArrayBuffer, an array buffer view, ReadableStream, Blob, or File.",
      );
    } else {
      assert(this.#key);
      const key = this.#cryptoKey ??
        (this.#cryptoKey = await importKey(this.#key));
      const iv = globalThis.crypto.getRandomValues(new Uint8Array(12));
      if (
        ArrayBuffer.isView(blob) || blob instanceof ArrayBuffer ||
        blob instanceof SharedArrayBuffer
      ) {
        return concat([
          iv,
          new Uint8Array(
            await globalThis.crypto.subtle.encrypt(
              { name: "AES-GCM", iv },
              key,
              blob,
            ),
          ),
        ]);
      } else if (blob instanceof Blob) {
        const buffer = await blob.arrayBuffer();
        if (blob instanceof File) {
          const { type, name, lastModified } = blob;
          return new File(
            [
              iv,
              await globalThis.crypto.subtle.encrypt(
                { name: "AES-GCM", iv },
                key,
                buffer,
              ),
            ],
            name,
            { type, lastModified },
          );
        }
        const { type } = blob;
        return new Blob([
          iv,
          await globalThis.crypto.subtle.encrypt(
            { name: "AES-GCM", iv },
            key,
            buffer,
          ),
        ], { type });
      }
      throw TypeError(
        "Blobs must be an ArrayBuffer, an array buffer view, ReadableStream, Blob, or File.",
      );
    }
  }

  async #decrypt(blob: Uint8Array) {
    if (this.#encryptor) {
      return this.#encryptor.decrypt(blob);
    } else {
      assert(this.#key);
      const key = this.#cryptoKey ??
        (this.#cryptoKey = await importKey(this.#key));
      const iv = blob.slice(0, 12);
      const message = blob.slice(12);
      return new Uint8Array(
        await globalThis.crypto.subtle.decrypt(
          { name: "AES-GCM", iv },
          key,
          message,
        ),
      );
    }
  }

  constructor(kv: Deno.Kv, encryptWith: string | Uint8Array | Encryptor) {
    this.#kv = kv;
    if (isEncryptor(encryptWith)) {
      this.#encryptor = encryptWith;
    } else {
      this.#key = encryptWith;
    }
  }

  /**
   * Retrieve a {@linkcode Deno.KvEntryMaybe} for the supplied key. If the
   * entry is present, it will be resolved as a {@linkcode Blob} or
   * {@linkcode File}. If the blob value was originally a `File` a `File` will
   * be returned, otherwise a `Blob`.
   *
   * @example
   *
   * ```ts
   * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
   *
   * const kv = await openCryptoKv(generateKey());
   * const maybeValue = await kv.getBlob(["hello"], { blob: true });
   * // do something with maybeValue
   * kv.close();
   * ```
   */
  getBlob(
    key: Deno.KvKey,
    options: { consistency?: Deno.KvConsistencyLevel | undefined; blob: true },
  ): Promise<Deno.KvEntryMaybe<Blob | File>>;
  /**
   * Retrieve a {@linkcode Deno.KvEntryMaybe} for the supplied key. If the
   * entry is present, it will be resolved as an {@linkcode Uint8Array}
   *
   * @example
   *
   * ```ts
   * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
   *
   * const kv = await openCryptoKv(generateKey());
   * const maybeValue = await kv.getBlob(["hello"]);
   * // do something with maybeValue
   * kv.close();
   * ```
   */
  getBlob(
    key: Deno.KvKey,
    options?: {
      consistency?: Deno.KvConsistencyLevel | undefined;
      blob?: boolean;
    },
  ): Promise<Deno.KvEntryMaybe<Uint8Array>>;
  async getBlob(key: Deno.KvKey, options: {
    consistency?: Deno.KvConsistencyLevel | undefined;
    blob?: boolean;
  } = {}): Promise<
    Deno.KvEntryMaybe<ReadableStream<Uint8Array> | Uint8Array | File | Blob>
  > {
    const meta = await asMeta(this.#kv, key, options);
    if (!meta.value || !meta.value.encrypted) {
      return { key, value: null, versionstamp: null };
    }
    if (options.blob) {
      const value = await this.#asBlob(key, options, meta.value);
      return value
        ? { key: [...key], value, versionstamp: meta.versionstamp }
        : { key: [...key], value: null, versionstamp: null };
    }
    const message = await asUint8Array(this.#kv, key, options);
    if (!message) {
      return { key, value: null, versionstamp: null };
    }
    const value = await this.#decrypt(message);
    assert(value);
    return {
      key: [...key],
      value,
      versionstamp: meta.versionstamp,
    };
  }

  /**
   * Resolve with just the value of an encrypted blob as a {@linkcode Blob} or
   * {@linkcode File}. If there isn't an encrypted blob value associated with
   * the key, `null` will be resolved. If the blob was originally a `File` a
   * `File` will be resolved, otherwise a `Blob`.
   *
   * @example
   *
   * ```ts
   * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
   *
   * const kv = await openCryptoKv(generateKey());
   * const value = await kv.getAsBlob(["hello"]);
   * if (value) {
   *   // do something with value
   * }
   * kv.close();
   * ```
   */
  async getAsBlob(
    key: Deno.KvKey,
    options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
  ): Promise<Blob | File | null> {
    // TODO: provide the ability to return a Response using the Blob
    const meta = await asMeta(this.#kv, key, options);
    if (!meta.value?.encrypted) {
      return null;
    }
    return this.#asBlob(key, options, meta.value);
  }

  /**
   * Resolve with just the value of an encrypted blob as a {@linkcode BlobJSON}.
   * If there isn't an encrypted blob value associated with the key, `null` will
   * be resolved.
   *
   * @example Retrieve a JSON object from the store
   *
   * ```ts
   * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
   *
   * const kv = await openCryptoKv(generateKey());
   * const value = await kv.getAsJSON(["hello"]);
   * if (value) {
   *  // do something with value
   * }
   * kv.close();
   * ```
   */
  async getAsJSON(
    key: Deno.KvKey,
    options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
  ): Promise<BlobJSON | null> {
    const meta = await asMeta(this.#kv, key, options);
    if (!meta.value?.encrypted) {
      return null;
    }
    const blob = await this.#asBlob(key, options, meta.value);
    return blob ? toJSON(blob) : null;
  }

  /**
   * Retrieve the meta data associated with a blob value for the provided key.
   * If the entry is not is not present, not a blob, or not encrypted `null`
   * will be resolved as the value.
   *
   * @example
   *
   * ```ts
   * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
   *
   * const kv = await openCryptoKv(generateKey());
   * const meta = await kv.getBlobMeta(["hello"]);
   * if (meta.value) {
   *   // do something with meta
   * }
   * kv.close();
   * ```
   */
  async getBlobMeta(
    key: Deno.KvKey,
    options: { consistency?: Deno.KvConsistencyLevel | undefined } = {},
  ): Promise<Deno.KvEntryMaybe<BlobMeta>> {
    const maybeMeta = await asMeta(this.#kv, key, options);
    if (!maybeMeta.value?.encrypted) {
      return { key: maybeMeta.key, value: null, versionstamp: null };
    }
    return maybeMeta;
  }

  /**
   * Create or update an encrypted blob entry in the KV store.
   *
   * The value can be an {@linkcode ArrayBuffer}, typed array,
   * {@linkcode DataView}, {@linkcode Blob}, or {@linkcode File}.
   *
   * The function encrypts the value and stores it in chunks within the store
   * ensuring no individual chunk exceeds the value size limitations imposed by
   * Deno KV.
   *
   * Optionally an `expireIn` option can be specified to set a time-to-live
   * (TTL) for the key. The TTL is specified in milliseconds, and the key will
   * be deleted from the database at earliest after the specified number of
   * milliseconds have elapsed. Once the specified duration has passed, the
   * key may still be visible for some additional time. If the `expireIn`
   * option is not specified, the key will not expire.
   *
   * @example
   *
   * ```ts
   * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
   *
   * const kv = await openCryptoKv(generateKey());
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
  async setBlob(
    key: Deno.KvKey,
    blob:
      | ArrayBufferView
      | ArrayBufferLike
      | Blob
      | File,
    options?: { expireIn?: number },
  ): Promise<Deno.KvCommitResult> {
    const items = await keys(this.#kv, { prefix: [...key, BLOB_KEY] });
    const value = await this.#encrypt(blob);
    let operation = batchedAtomic(this.#kv);
    operation = await setBlob(operation, key, value, items.length, {
      ...options,
      encrypted: true,
    });
    const res = await operation.commit();
    if (!res[0].ok) {
      throw new Error("Unexpected error when setting blob.");
    }
    return res[0];
  }

  /**
   * Remove/delete a binary object from the store with a given key that has been
   * {@linkcode set}.
   *
   * @example
   *
   * ```ts
   * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
   *
   * const kv = await openCryptoKv(generateKey());
   * await kv.deleteBlob(["hello"]);
   * kv.close();
   * ```
   */
  deleteBlob(key: Deno.KvKey): Promise<void> {
    return removeBlob(this.#kv, key);
  }

  /**
   * Close the database connection. This will prevent any further operations
   * from being performed on the database, and interrupt any in-flight
   * operations immediately.
   */
  close(): void {
    this.#kv.close();
  }
}

/**
 * Generate a new random encryption key which can be used with
 * {@linkcode CryptoKv}. The value returned as a hex encoded string.
 *
 * By default, the key length is 256 bits, but the `bitLength` can be supplied
 * that is 128, 192 or 256 bits.
 *
 * @example
 *
 * ```ts
 * import { generateKey } from "jsr:@kitsonk/kv-toolbox/crypto";
 *
 * const key = generateKey();
 * ```
 */
export function generateKey(bitLength: 128 | 192 | 256 = 256): string {
  if (![128, 192, 256].includes(bitLength)) {
    throw new RangeError("Bit length must be 128, 192, or 256.");
  }
  const raw = globalThis.crypto.getRandomValues(new Uint8Array(bitLength / 8));
  return encodeHex(raw);
}

/**
 * Open a Deno KV store and resolves with an instance of {@linkcode CryptoKv}
 * which can be used to deal with encrypted values.
 *
 * `encryptWith` can be a hex encoded string or {@linkcode Uint8Array} that is
 * 128, 192, or 256 bits in length, or an object which conforms to the
 * {@linkcode Encryptor} interface.
 *
 * When a path is provided pointing to the local file system, the database will
 * be persisted to disk at that path. Read and write access to the file is
 * required. If it is a remote connection, Deno will connect to that database.
 * Net access to that host is required.
 *
 * When no path is provided, the database will be opened in a default path for
 * the current script. This location is persistent across script runs and is
 * keyed on the origin storage key (the same key that is used to determine
 * `localStorage` persistence). More information about the origin storage key
 * can be found in the Deno Manual.
 *
 * @example
 *
 * ```ts
 * import { generateKey, openCryptoKv } from "jsr:@kitsonk/kv-toolbox/crypto";
 *
 * const kv = await openCryptoKv(generateKey());
 * // kv is now an instance of CryptoKv
 * ```
 */
export async function openCryptoKv(
  encryptWith: string | Uint8Array | Encryptor,
  path?: string | undefined,
): Promise<CryptoKv> {
  const kv = await Deno.openKv(path);
  return new CryptoKv(kv, encryptWith);
}
