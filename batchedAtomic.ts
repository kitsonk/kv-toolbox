/**
 * Provides the function {@linkcode batchedAtomic} which is like
 * `Deno.Kv#atomic()` but will work around the limitation 10 transactions per
 * atomic operation.
 *
 * @module
 */

/** The default batch size for atomic operations. */
const BATCH_SIZE = 10;

type AtomicOperationKeys = keyof Deno.AtomicOperation;

export class BatchedAtomicOperation {
  #batchSize: number;
  #kv: Deno.Kv;
  // deno-lint-ignore no-explicit-any
  #queue: [AtomicOperationKeys, any[]][] = [];

  #enqueue<Op extends AtomicOperationKeys>(
    operation: Op,
    args: Parameters<Deno.AtomicOperation[Op]>,
  ): this {
    this.#queue.push([operation, args]);
    return this;
  }

  constructor(
    kv: Deno.Kv,
    { batchSize = BATCH_SIZE }: { batchSize?: number } = {},
  ) {
    this.#kv = kv;
    this.#batchSize = batchSize;
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
   */
  set(key: Deno.KvKey, value: unknown, options?: { expireIn?: number }): this {
    return this.#enqueue("set", [key, value, options]);
  }

  /**
   * Add to the operation a mutation that deletes the specified key if all
   * checks pass during the commit.
   */
  delete(key: Deno.KvKey): this {
    return this.#enqueue("delete", [key]);
  }

  /**
   * Add to the operation a mutation that enqueues a value into the queue if all
   * checks pass during the commit.
   */
  enqueue(
    value: unknown,
    options?: { delay?: number; keysIfUndelivered?: Deno.KvKey[] },
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
    let count = 0;
    let operation = this.#kv.atomic();
    let hasCheck = false;
    while (this.#queue.length) {
      const [method, args] = this.#queue.shift()!;
      count++;
      if (method === "check") {
        hasCheck = true;
      }
      // deno-lint-ignore no-explicit-any
      (operation[method] as any).apply(operation, args);
      if (count >= this.#batchSize || !this.#queue.length) {
        const rp = operation.commit();
        results.push(rp);
        if (this.#queue.length) {
          if (hasCheck) {
            const result = await rp;
            if (!result.ok) {
              break;
            }
          }
          count = 0;
          operation = this.#kv.atomic();
        }
      }
    }
    return Promise.all(results);
  }
}

/** Similar to `Deno.Kv#atomic()` but deals with the limit of transactions
 * allowed per atomic operation.
 *
 * When committing the transaction, the operation is broken up in batches and
 * each commit result from each batch is returned, unless there is a commit
 * error, where any pending batched operations will be abandoned and the last
 * item in the commit result array will be the error.
 *
 * By default, the batch size is `10` but can be supplied in the `options`
 * property of `batchSize`. */
export function batchedAtomic(kv: Deno.Kv, options?: { batchSize?: number }) {
  return new BatchedAtomicOperation(kv, options);
}
