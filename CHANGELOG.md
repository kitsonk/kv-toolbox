# kv-toolbox change log

## Version 0.16.3

- fix: use sizeOf instead of V8 serialize (7db9e63)

## Version 0.16.2

- fix: change max atomic transaction byte limit (b89d680)
- chore: exclude tests from publish (bf75d36)

## Version 0.16.1

- feat: make batched_atomic loadable on web (798d991)

## Version 0.16.0

- feat: re-work batching for batchedAtomic to align to current Deno KV (e3c8136)

  `batchedAtomic()` now aligns to current versions of Deno KV in how it decides
  where to _segment_ atomic transactions. Because of the much higher increases
  users should consider only using `batchedAtomic()` when dealing with
  potentially large transactions where potentially failing due to the size
  restriction is awkward or difficult.

- feat: align blob set and get to Deno.Kv APIs (923faf1)

  **BREAKING** Previously `set()` resolved with void and `get()` resolved with a
  value or `null`. In addition, `getMeta()` resolved with a value.

  Now `set()` resolves with a `Deno.KvCommitResult` and `get()` and `getMeta()`
  resolve with a `Deno.KvEntryMaybe` with the appropriate type.

- feat: add support for checking blobs in batched_atomic (389730a)

  `batchedAtomic()` transactions now support `.checkBlob()` checks as part of an
  atomic transaction.

- feat: add `getAsResponse()` to blob (796ed64)

  `getAsResponse()` will retrieve a blob entry as a `Response` which will stream
  the blob from the store to a client.

- chore: linting in blob_util (bd6b888)
- docs: update readme about batchAtomic (bca0fb6)
- docs: improvements to inline keys docs (023c79c)

## Version 0.15.0

- feat: add getMeta() for blobs (2d1e060)

## Version 0.14.0

- feat: store size for blobs (34b8aa1)

## Version 0.13.0

- feat: add blob support to uniqueCount (2a0c155)

## Version 0.12.0

- feat: add JSON support for blob (ad680c9)
