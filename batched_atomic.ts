/**
 * Provides the function {@linkcode batchedAtomic} which is like
 * {@linkcode Deno.Kv.prototype.atomic} but will work around the per atomic
 * transaction limits imposed by Deno KV.
 *
 * It also supports `setBlob()` and `checkBlob()` to allow setting of checking
 * of kv-toolbox blob values as part of a transaction.
 *
 * In the past, Deno KV had very low limits (like 10 mutations per transaction)
 * but those limits have been changed to far more reasonable levels, so in most
 * cases {@linkcode batchedAtomic} is not needed. The only _advantage_ is that
 * you can make arbitrarily large atomic transactions and not worry about
 * having to deal with a limit failure in code. But most users should consider
 * just dealing with {@linkcode Deno.Kv.prototype.atomic} directly.
 *
 * @example
 *
 * ```ts
 * import { batchedAtomic } from "@kitsonk/kv-toolbox/batched_atomic";
 *
 * const kv = await Deno.openKv();
 * await batchedAtomic(kv)
 *   .check({ key: ["hello"], versionstamp: null })
 *   .set(["hello"], "deno kv")
 *   .commit();
 * await kv.close();
 * ```
 *
 * @module
 */

import { estimateSize } from "@deno/kv-utils/estimate-size";

import { BLOB_KEY, BLOB_META_KEY, setBlob } from "./blob_util.ts";
import { keys } from "./keys.ts";

interface KVToolboxAtomicOperation extends Deno.AtomicOperation {
  deleteBlob(key: Deno.KvKey): this;

  setBlob(
    key: Deno.KvKey,
    value: ArrayBufferLike | ReadableStream<Uint8Array> | Blob,
    options?: { expireIn?: number },
  ): this;
}

type AtomicOperationKeys = keyof KVToolboxAtomicOperation;

// These are indicated from deno/ext/kv/lib.rs and are current as of 524e451
// We have to use slightly less numbers, because we can only estimate byte
// lengths and in some cases we underestimate the size of keys and values
const MAX_CHECKS = 99;
const MAX_MUTATIONS = 999;
const MAX_TOTAL_MUTATION_SIZE_BYTES = 750_000;
const MAX_TOTAL_KEY_SIZE_BYTES = 75_000;

/**
 * The class that encapsulates the batched atomic operations. Works around
 * limitations imposed by Deno KV related to individual atomic operations.
 */
export class BatchedAtomicOperation {
  #kv: Deno.Kv;
  #maxChecks: number;
  #maxBytes: number;
  #maxKeyBytes: number;
  #maxMutations: number;
  // deno-lint-ignore no-explicit-any
  #queue: [AtomicOperationKeys, any[]][] = [];

  #enqueue<Op extends AtomicOperationKeys>(
    operation: Op,
    args: Parameters<KVToolboxAtomicOperation[Op]>,
  ): this {
    this.#queue.push([operation, args]);
    return this;
  }

  constructor(
    kv: Deno.Kv,
    {
      maxChecks = MAX_CHECKS,
      maxMutations = MAX_MUTATIONS,
      maxBytes = MAX_TOTAL_MUTATION_SIZE_BYTES,
      maxKeyBytes = MAX_TOTAL_KEY_SIZE_BYTES,
    }: {
      maxChecks?: number;
      maxMutations?: number;
      maxBytes?: number;
      maxKeyBytes?: number;
    } = {},
  ) {
    this.#kv = kv;
    this.#maxChecks = maxChecks;
    this.#maxBytes = maxBytes;
    this.#maxKeyBytes = maxKeyBytes;
    this.#maxMutations = maxMutations;
  }

  /**
   * Add to the operation a check that ensures that the versionstamp of the
   * key-value pair in the KV store matches the given versionstamp. If the check
   * fails, the entire operation will fail and no mutations will be performed
   * during the commit.
   *
   * If there are additional batches of atomic operations to perform, they will
   * be abandoned.
   */
  check(...checks: Deno.AtomicCheck[]): this {
    return this.#enqueue("check", checks);
  }

  /**
   * Add to the operation a check that ensures that the versionstamp of the blob
   * key-value pair in the KV store matches the given versionstamp. If the check
   * fails, the entire operation will fail and no mutations will be performed
   * during the commit.
   *
   * The blob should have previously been set via kv-toolbox's `set()` or as
   * part of an batched atomic operation via `setBlob()`.
   *
   * If there are additional batches of atomic operations to perform, they will
   * be abandoned.
   */
  checkBlob(...checks: Deno.AtomicCheck[]): this {
    return this.#enqueue(
      "check",
      checks.map(({ key, versionstamp }) => ({
        key: [...key, BLOB_META_KEY],
        versionstamp,
      })),
    );
  }

  /**
   * Add to the operation a mutation that performs the specified mutation on
   * the specified key if all checks pass during the commit. The types and
   * semantics of all available mutations are described in the documentation for
   * {@linkcode Deno.KvMutation}.
   */
  mutate(...mutations: Deno.KvMutation[]): this {
    return this.#enqueue("mutate", mutations);
  }

  /**
   * Shortcut for creating a `sum` mutation. This method wraps `n` in a
   * {@linkcode Deno.KvU64}, so the value of `n` must be in the range
   * `[0, 2^64-1]`.
   */
  sum(key: Deno.KvKey, n: bigint): this {
    return this.#enqueue("sum", [key, n]);
  }

  /**
   * Shortcut for creating a `min` mutation. This method wraps `n` in a
   * {@linkcode Deno.KvU64}, so the value of `n` must be in the range
   * `[0, 2^64-1]`.
   */
  min(key: Deno.KvKey, n: bigint): this {
    return this.#enqueue("min", [key, n]);
  }

  /**
   * Shortcut for creating a `max` mutation. This method wraps `n` in a
   * {@linkcode Deno.KvU64}, so the value of `n` must be in the range
   * `[0, 2^64-1]`.
   */
  max(key: Deno.KvKey, n: bigint): this {
    return this.#enqueue("max", [key, n]);
  }

  /**
   * Add to the operation a mutation that sets the value of the specified key
   * to the specified value if all checks pass during the commit.
   *
   * Optionally an `expireIn` option can be specified to set a time-to-live
   * (TTL) for the key. The TTL is specified in milliseconds, and the key will
   * be deleted from the database at earliest after the specified number of
   * milliseconds have elapsed. Once the specified duration has passed, the
   * key may still be visible for some additional time. If the `expireIn`
   * option is not specified, the key will not expire.
   */
  set(key: Deno.KvKey, value: unknown, options?: { expireIn?: number }): this {
    return this.#enqueue("set", [key, value, options]);
  }

  /**
   * Add to the operation a mutation that sets a blob value in the store if all
   * checks pass during the commit. The blob can be any array buffer like
   * structure, a byte {@linkcode ReadableStream}, or a {@linkcode Blob} or
   * {@linkcode File}.
   */
  setBlob(
    key: Deno.KvKey,
    value: ArrayBufferLike | ReadableStream<Uint8Array> | Blob,
    options?: { expireIn?: number },
  ): this {
    return this.#enqueue("setBlob", [key, value, options]);
  }

  /**
   * Add to the operation a mutation that deletes the specified key if all
   * checks pass during the commit.
   */
  delete(key: Deno.KvKey): this {
    return this.#enqueue("delete", [key]);
  }

  /**
   * Add to the operation a set of mutations to delete the specified parts of
   * a blob value if all checks pass during the commit.
   */
  deleteBlob(key: Deno.KvKey): this {
    return this.#enqueue("deleteBlob", [key]);
  }

  /**
   * Add to the operation a mutation that enqueues a value into the queue if all
   * checks pass during the commit.
   */
  enqueue(
    value: unknown,
    options?: {
      delay?: number;
      keysIfUndelivered?: Deno.KvKey[];
      backoffSchedule?: number[];
    },
  ): this {
    return this.#enqueue("enqueue", [value, options]);
  }

  /**
   * Commit the operation to the KV store. Returns an array of values indicating
   * whether checks passed and mutations were performed. If the operation failed
   * because of a failed check, the last element of the return value will be a
   * {@linkcode Deno.KvCommitError} with an `ok: false` property. If the
   * operation failed for any other reason (storage error, invalid value, etc.),
   * the promise will be rejected with an exception. If the operation succeeded,
   * the return value will be an individual {@linkcode Deno.KvCommitResult}
   * object with a `ok: true` property and the versionstamp of the value
   * committed to KV broken up by the batch size, which defaults to `10`.
   *
   * If the commit returns `ok: false`, one may create a new atomic operation
   * with updated checks and mutations and attempt to commit it again. See the
   * note on optimistic locking in the documentation for
   * {@linkcode Deno.AtomicOperation}.
   */
  async commit(): Promise<(Deno.KvCommitResult | Deno.KvCommitError)[]> {
    if (!this.#queue.length) {
      return Promise.resolve([]);
    }
    const results: Promise<Deno.KvCommitResult | Deno.KvCommitError>[] = [];
    let checks = 0;
    let mutations = 0;
    let payloadBytes = 0;
    let keyBytes = 0;
    let operation = this.#kv.atomic();
    let hasCheck = false;
    while (this.#queue.length) {
      const [method, args] = this.#queue.shift()!;
      if (method === "setBlob") {
        const queue = this.#queue;
        this.#queue = [];
        const [key, value, options] = args as [
          Deno.KvKey,
          ArrayBufferLike | ReadableStream<Uint8Array> | Blob,
          { expireIn?: number } | undefined,
        ];
        const items = await keys(this.#kv, { prefix: [...key, BLOB_KEY] });
        await setBlob(this, key, value, items.length, options);
        this.#queue.push(...queue);
      } else if (method === "deleteBlob") {
        const [key] = args as [Deno.KvKey];
        const items = await keys(this.#kv, { prefix: [...key, BLOB_KEY] });
        for (const item of items) {
          this.#queue.unshift(["delete", [item]]);
        }
        this.#queue.unshift(["delete", [[...key, BLOB_META_KEY]]]);
      } else {
        if (method === "check") {
          checks++;
          for (const { key } of args as Deno.AtomicCheck[]) {
            const len = key.reduce(
              (prev: number, part: Deno.KvKeyPart) => prev + estimateSize(part),
              0,
            );
            payloadBytes += len;
            keyBytes += len;
          }
          hasCheck = true;
        } else {
          mutations++;
          if (method === "mutate") {
            for (const mutation of args as Deno.KvMutation[]) {
              const keyLen = estimateSize(mutation.key);
              payloadBytes += keyLen;
              keyBytes += keyLen;
              if (mutation.type === "set") {
                payloadBytes += estimateSize(mutation.value);
              } else if (mutation.type !== "delete") {
                payloadBytes += 8;
              }
            }
          } else if (method === "max" || method === "min" || method === "sum") {
            const [key] = args as [Deno.KvKey];
            const keyLen = estimateSize(key);
            keyBytes += keyLen;
            payloadBytes += keyLen + 8;
          } else if (method === "set") {
            const [key, value] = args as [Deno.KvKey, unknown];
            const keyLen = estimateSize(key);
            keyBytes += keyLen;
            payloadBytes += keyLen + estimateSize(value);
          } else if (method === "delete") {
            const [key] = args as [Deno.KvKey];
            const keyLen = estimateSize(key);
            keyBytes += keyLen;
            payloadBytes += keyLen;
          } else if (method === "enqueue") {
            const [value] = args as [unknown];
            payloadBytes += estimateSize(value);
          }
        }
        if (
          checks > this.#maxChecks || mutations > this.#maxMutations ||
          payloadBytes > this.#maxBytes || keyBytes > this.#maxKeyBytes
        ) {
          const rp = operation.commit();
          results.push(rp);
          if (hasCheck) {
            const result = await rp;
            if (!result.ok) {
              break;
            }
          }
          checks = 0;
          mutations = 0;
          payloadBytes = 0;
          keyBytes = 0;
          operation = this.#kv.atomic();
        }
        // deno-lint-ignore no-explicit-any
        (operation[method] as any).apply(operation, args);
        if (!this.#queue.length) {
          const rp = operation.commit();
          results.push(rp);
        }
      }
    }
    return Promise.all(results);
  }
}

/**
 * Options which can be adjusted when using a batched atomic.
 *
 * These all default to the current values used by Deno, so typically these
 * never need to be set unless you specifically know what you are doing!
 */
export interface BatchAtomicOptions {
  /**
   * Deno KV limits the number of checks per atomic transaction. This changes
   * the default of 99.
   */
  maxChecks?: number;
  /**
   * Deno KV limits the number of mutations per atomic transactions. This
   * changes the default of 999.
   */
  maxMutations?: number;
  /**
   * Deno KV limits the overall byte size of an atomic transaction, which
   * includes data for checks and mutations. This changes the default of 800k.
   *
   * There is also the limit of 64K per value.
   */
  maxBytes?: number;
  /**
   * Deno KV limits the total byte size of keys associated with an atomic
   * transaction. This changes the default of 80k.
   */
  maxKeyBytes?: number;
}

/**
 * Similar to {@linkcode Deno.Kv.prototype.atomic} but deals with the limits of
 * transactions imposed by Deno KV.
 *
 * When committing the transaction, the operation is broken up in batches and
 * each commit result from each batch is returned, unless there is a commit
 * error, where any pending batched operations will be abandoned and the last
 * item in the commit result array will be the error.
 */
export function batchedAtomic(
  kv: Deno.Kv,
  options?: BatchAtomicOptions,
): BatchedAtomicOperation {
  return new BatchedAtomicOperation(kv, options);
}
