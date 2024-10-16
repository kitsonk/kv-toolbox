# kv-toolbox change log

## Version 0.20.1

- fix: make json module browser compatible (422df76)

## Version 0.20.0

- feat: arrays, objects, maps and sets are deeply serialized (bf8f285)

  Previously only value supported by JSON directly were serialized as values and
  keys of arrays, objects, maps and sets. While kv-toolbox can deserialize the
  old format, it supports a new JSON format that allows all value supported by
  Deno KV to be properly serialized and deserialized.

- docs: update changelog (38bd7cd)

## Version 0.19.1

- fix: properly handle `Infinity` and `NaN` (410d214)
- chore: type updates for Deno 2 (b6e565b)

## Version 0.19.0

- feat: add toolbox APIs (1e0842f)
- feat: add default export (0600060)
- feat: add ability to retrieve response with toolbox (b0281f6)
- feat: add blob as JSON to toolbox (6bf5d7d)
- fix: default export (335bcd7)
- fix: add filename to export response on toolbox (af2441a)
- chore: update deps and Deno v2 (64bfe85)
- chore: update crypto bench (2485218)
- docs: update verbiage in README (#14)
- docs: fix inline doc refs (72c580b)

## Version 0.18.0

- feat: support DataView in JSON (3ce809b)
- feat: initial implementation of crypto (77f0df0)
- chore: add superserial to byte_size bench (2e9aa1e)
- chore: update std version (b519f94)
- tests: fix fragile size_of test (c33207e)

## Version 0.17.0

- feat: add `toBlob()` (9b874dc)
- fix: type check blob set values (f2aa18c)
- fix: harden reading blob parts out of store (89e7de3)

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
