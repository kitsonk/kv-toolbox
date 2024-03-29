# kv-toolbox

A set of tools for working with Deno KV.

## Batched Atomic

A set of APIs for dealing with the limitation of atomic commit sized in Deno KV,
where currently only 10 operations operations can be part of a commit.

### `batchedAtomic()`

Similar to `Deno.Kv#atomic()`, but will batch individual transactions across as
many atomic operations as necessary.

There are two additional methods supported on batched atomics not supported by
Deno KV atomic transactions:

- `.setBlob(key, value, options?)` - Allows setting of arbitrarily size blob
  values as part of an atomic transaction. The values can be a byte
  `ReadableStream`, array buffer like, a `Blob` or a `File`. It will work around
  the constraints of Deno KV value sizes by splitting the value across multiple
  keys.

- `.deleteBlob(key)` - Allows deletion of all parts of a blob value as part of
  an atomic transaction.

The `commit()` method will return a promise which resolves with an array of
results based on how many batches the operations was broken up into.

## Blob

A set of APIs for storing arbitrarily sized blobs in Deno KV. Currently Deno KV
has a limit of key values being 64k. The `set()` function breaks down a blob
into chunks and manages sub-keys to store the complete value. The `get()`,
`getAsBlob()` and `getAsStream()` functions reverse that process, and `remove()`
will delete the key, sub-keys and values.

### `set()`

Similar to `Deno.Kv.prototype.set()`, in that it stores a blob value with an
associated key. In order to deal with the size limitations of values, `set()`
will transparently chunk up the blob into parts that can be handled by Deno KV.

The blob can be a byte `ReadableStream`, array buffer like, a `Blob` or a
`File`.

When the value is being set is a `Blob` or `File` the meta data will also be
preserved (like the `type` property).

### `get()`

Similar to `Deno.Kv.prototype.get()`, in that it retrieves a blob value based on
the provided key. If a previous blob value has been set with `set()`, it will be
retrieved.

By default the value is resolved as a `Uint8Array` but if the option `stream` is
set to `true`, then a byte `ReadableStream` is provided to read out the blob. If
the option `blob` is set to `true`, then a `Blob` or `File` will be resolved. If
the value originally set was a `File` or `Blob` the resolved instance will be
that of the original value including restoring the additional properties, like
`type`. If the value wasn't a `Blob` or `File` originally, the function will
resolve to a `Blob` with an empty `type`.

### `getAsBlob()`

Retrieves a blob value based on the provided key and resolves with a `Blob` or
`File`. If the value originally set was a `File` or `Blob` the resolved instance
will be that of the original value including restoring the additional
properties, like `type`. If the value wasn't a `Blob` or `File` originally, the
function will resolve to a `Blob` with an empty `type`.

### `getAsJSON()`

Retrieve a blob value based on the provided key and resolve with its JSON
representation.

### `getAsStream()`

Retrieves a blob value based on the provided key and returns a byte
`ReadableStream` which the binary data can be read from. If there is no value
present, the stream will be empty.

### `getMeta()`

Retrieves the meta data of a blob value based on the key provided. If the data
isn't available `null` is resolved.

### `remove()`

Similar to `Deno.Kv.prototype.delete()`, in that it deletes a blob key and value
form the data store. If a blob value isn't stored for the given key, it resolves
in a noop.

### `toJSON()`

Convert an array buffer, typed array, `Blob` or `File` to a format which can be
stringified into a JSON string.

### `toValue()`

Convert a JSON representation of a blob back into a value. Depending on what
kind of value the JSON represents, either a `Uint8Array`, `Blob` or `File` is
returned.

## JSON

APIs allowing the serialization and deserialization of Deno KV entries, keys,
and values as JSON.

These API are useful when trying to share information to or from the Deno
runtime, like for example with a browser client. They can also be useful when
wanting to start information from a Deno KV store in a human readable text
format.

### `entryMaybeToJSON()`

Serialize a `Deno.KvEntryMaybe` to a structure which can be safely converted to
a JSON string.

### `entryToJSON()`

Serialize a `Deno.KvEntry` to a structure which can be safely converted to a
JSON string.

### `keyPartToJSON()`

Serialize a `Deno.KvKeyPart` to a structure which can be safely converted to a
JSON string.

### `keyToJSON()`

Serialize a `Deno.KvKey` to a structure which can be safely converted to a JSON
string.

### `toEntry()`

Deserialize a JSON structure to a `Deno.KvEntry`.

### `toEntryMaybe()`

Deserialize a JSON structure to a `Deno.KvEntryMaybe`.

### `toKey()`

Deserialize a JSON structure to a `Deno.KvKey`.

### `toKeyPart()`

Deserialize a JSON structure to a `Deno.KvKeyPart`.

### `toValue()`

Deserialize a JSON structure to a value which can be stored in a Deno KV store.

### `valueToJSON()`

Serialize a value which has been stored in a Deno KV store into a structure
which can be safely converted to a JSON string.

## NDJSON

New line delimitated JSON ([NDJSON](https://github.com/ndjson/ndjson-spec)) is a
standard for supporting JSON string encoding of data where each record of data
is delimitated by a new line. This particular format is the most straight-
forward way of supporting JSON encoding and streaming.

The toolbox includes the capabilities to export entries from a KV store to
NDJSON, transform a byte stream of NDJSON into individual JSON KV entry
representations, and be able to import KV entries from NDJSON encoded data.

### `exportEntries()`

Like `Deno.Kv.prototype.list()`, but entries are returned as a stream of bytes
or strings encoded as NDJSON.

### `exportToResponse()`

Like `Deno.Kv.prototype.list()`, but a `Response` is returned with the selected
entries encoded as NDJSON as the body of the response, suitable for sending to a
client as a response to a query.

### `LineTransformStream()`

A transform stream which takes a byte stream, like from a `Request` body, of
NDJSON encoded entry data and transforms it into individual chunks of JSON
strings which can be used with `JSON.parse()`.

### `importEntries()`

Takes NDJSON encoded data and imports it into a Deno KV store.

## Keys

APIs for dealing with Deno KV keys.

### `equals()`

Compares the equality of two `Deno.KvKey`s, returning `true` if they are equal
and `false` if they are not. This is more specialized than other forms of deeply
equal comparison.

### `partEquals()`

Compares the equality of two `Deno.KvKeyPart`s, returning `true` if they are
equal and `false` if they are not. This is more specialized than other forms of
equality comparison.

### `startsWith()`

Determines if the `key` starts with the `prefix` provided, returning `true` if
it does, otherwise `false`.

### `keys()`

Similar to `Deno.Kv.prototype.list()`, in that is takes a selector, but instead
of returning an async iterator of matched values, it resolves with an array of
matching keys.

### `tree()`

This resolves with the key parts in an tree like structure, where each branch of
the tree contains the children indexed by key part. The interface of the
resolved value is:

```ts
interface KeyTreeNode {
  part: Deno.KvKeyPart;
  children?: KeyTreeNode[];
}

interface KeyTree {
  prefix: Deno.KvKey;
  children?: KeyTreeNode[];
}
```

### `unique()`

Resolves with an array of unique sub keys/prefixes for the provided prefix.

This is useful when storing keys and values in a hierarchical/tree view, where
you are retrieving a list and you want to know all the unique _descendants_ of a
key in order to be able to enumerate them.

### `uniqueCount()`

Resolves with an array of values which contain the unique sub keys/prefixes for
the provided prefix along with a count of how many keys there are.

This is useful when storing keys and values in a hierarchical/tree view, where
you are retrieving a list and you want to know all the unique _descendants_ of a
key (and the count of keys that match that prefix) in order to be able to
enumerate them or provide information about them.

---

Copyright 2023 - 2024 Kitson P. Kelly - All rights reserved.

MIT License.
