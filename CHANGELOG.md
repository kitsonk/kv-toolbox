# kv-toolbox change log

## Version 0.29.2

- fix: issue with cursors and blobs (74467f9)

## Version 0.29.1

- fix: cursors with query and blob lists (04b8f66)

## Version 0.29.0

- feat: add blob support to query (f5dae8a)

## Version 0.28.1

- fix: batched_atomic: properly handle multiple mutations (4d02534)
- docs: BlobKvListOptions (35e6810)

## Version 0.28.0

- feat: support listing of blobs (84010a4)

## Version 0.27.4

- fix: uniqueCount handles falsey keyparts properly (ac1d5da)

## Version 0.27.3

- feat: add limit to query (e3fc987)
- fix: properly handle `ArrayBufferLike` and `ArrayBufferView` (6e0365f)
- chore: update ci to Deno 2.2 (7177b14)

## Version 0.27.2

- feat: add query by kind of (b0baba9)

## Version 0.27.1

- feat: add db property to toolbox (5666c95)

## Version 0.27.0

- feat: add JSON serialization to query (23da00b)
- docs: fixup inline query examples (b58731b)

## Version 0.26.0

- feat: integrate keys and query (8b6eabd)

## Version 0.25.0

- feat: add query module (fb3fa44)
- feat: add matches op to query (2e23829)
- docs: improve query docs (0f1bce7)

## Version 0.24.1

- refactor: migrate fully to import maps (030d205)
- chore: add test result upload to ci (a0abb76)
- chore: update dependencies (b3699cf)

## Version 0.24.0

- feat: migrate import/export to `@deno/kv-utils` (aab2420)
- docs: type check inline documentation (deee889)
- chore: only CI against Deno 2 (f9828ce)
- chore: add Deno canary to CI (84a78ee)

## Version 0.23.0

- feat: migrate to @deno/kv-utils (68f23a0)

## Version 0.22.0

- feat: add `byteLength` to JSON serialization of byte arrays (2615a07)

## Version 0.21.1

- fix: add `.watch()` method to toolbox (d7c916f)

## Version 0.21.0

- feat: make sizeOf public (738b6bf)

  `sizeOf()` was previously just internal, but it is generally useful when working with Deno KV to be able to estimate
  the size of keys and values. The Deno KV documentation currently suggests using the length of `JSON.stringify()`
  string, which can be very problematic when dealing with complex values that don't serialize to JSON but are storable
  in Deno KV.

## Version 0.20.1

- fix: make json module browser compatible (422df76)

## Version 0.20.0

- feat: arrays, objects, maps and sets are deeply serialized (bf8f285)

  Previously only value supported by JSON directly were serialized as values and keys of arrays, objects, maps and sets.
  While kv-toolbox can deserialize the old format, it supports a new JSON format that allows all value supported by Deno
  KV to be properly serialized and deserialized.

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

  `batchedAtomic()` now aligns to current versions of Deno KV in how it decides where to _segment_ atomic transactions.
  Because of the much higher increases users should consider only using `batchedAtomic()` when dealing with potentially
  large transactions where potentially failing due to the size restriction is awkward or difficult.

- feat: align blob set and get to Deno.Kv APIs (923faf1)

  **BREAKING** Previously `set()` resolved with void and `get()` resolved with a value or `null`. In addition,
  `getMeta()` resolved with a value.

  Now `set()` resolves with a `Deno.KvCommitResult` and `get()` and `getMeta()` resolve with a `Deno.KvEntryMaybe` with
  the appropriate type.

- feat: add support for checking blobs in batched_atomic (389730a)

  `batchedAtomic()` transactions now support `.checkBlob()` checks as part of an atomic transaction.

- feat: add `getAsResponse()` to blob (796ed64)

  `getAsResponse()` will retrieve a blob entry as a `Response` which will stream the blob from the store to a client.

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
