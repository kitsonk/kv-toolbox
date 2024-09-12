# kv-toolbox

A set of tools for working with Deno KV.

## Toolbox

The default export of the library is the encapsulation of major functionality of
the library into classes which enhance the capabilities of working with a Deno
KV store, which are also available as individual named exports of the the
library.

There are to variants of the toolbox: `KvToolbox` and `CryptoKvToolbox`. These
provide all the APIs of a `Deno.Kv` and the additional APIs offered by the rest
of the library. The `CryptoKvToolbox` also attempts to encrypt and decrypt blob
values.

Opening a toolbox is similar to opening a `Deno.Kv` store:

```ts
import { openKvToolbox } from "jsr:@kitsonk/kv-toolbox";

const kv = await openKvToolbox();
```

If an encryption key is passed as an option, a `CryptoKvToolbox` instance will
be returned, where when storing and retrieving blobs in the store, they will be
encrypted and decrypted by default:

```ts
import { generateKey, openKvToolbox } from "jsr:@kitsonk/kv-toolbox";

const encryptWith = generateKey();
const kv = await openKvToolbox({ encryptWith });
```

> [!NOTE]
> In practice, encryption keys would need to be persisted from session to
> session. The code above would generate a new key every execution and any
> values stored could not be decrypted. To be practical, generated encryption
> keys need to be stored securely as a secret.

## Batched Atomic

A set of APIs for dealing with the limitation of atomic commit sizes in Deno KV.
Deno KV limits the number of checks and mutations as well as the overall byte
size of each commit and the byte size of the keys.

These limits are currently high for most workloads, but if you are dealing with
large transactions where if you need to perform more than 100 checks, 1000
mutations, using over 80k of key sizes or have an overall payload of over 800k,
then `batchedAtomic()` will avoid the transaction throwing by breaking up the
transaction into as many separate commits as necessary.

### `batchedAtomic()`

Similar to `Deno.Kv#atomic()`, but will batch individual transactions across as
many atomic operations as necessary.

There are three additional methods supported on batched atomics not supported by
Deno KV atomic transactions:

- `.checkBlob({ key, versionstamp })` - Allows performing checks on blob entries
  previously set with blob's `set()` function or via `.setBlob()` as part of a
  transaction.

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

The resolved `Deno.KvCommitResult` will contain the `versionstamp` of the blob's
meta data, which can be used for consistency checks.

### `get()`

Similar to `Deno.Kv.prototype.get()`, in that it retrieves a blob entry based on
the provided key. If a previous blob value has been set with `set()`, it will be
retrieved.

By default the value of the entry is resolved as a `Uint8Array` but if the
option `stream` is set to `true`, then a byte `ReadableStream` is provided to
read out the blob. If the option `blob` is set to `true`, then a `Blob` or
`File` will be the value of the entry. If the value originally set was a `File`
or `Blob` the resolved value instance will be that of the original value
including restoring the additional properties, like `type`. If the value wasn't
a `Blob` or `File` originally, the function will resolve to a `Blob` with an
empty `type`.

### `getAsBlob()`

Retrieves a blob value based on the provided key and resolves with a `Blob` or
`File`. If the value originally set was a `File` or `Blob` the resolved instance
will be that of the original value including restoring the additional
properties, like `type`. If the value wasn't a `Blob` or `File` originally, the
function will resolve to a `Blob` with an empty `type`.

### `getAsJSON()`

Retrieve a blob value based on the provided key and resolve with its JSON
representation.

### `getAsResponse()`

Retrieve a blob entry as a `Response` which will stream the value from the store
to a client. If the entry does not exist, the response will be a
`404 Not Found`. There are several options to configure how the response is set.

### `getAsStream()`

Retrieves a blob value based on the provided key and returns a byte
`ReadableStream` which the binary data can be read from. If there is no value
present, the stream will be empty.

### `getMeta()`

Retrieves the meta data entry of a blob value based on the key provided. The
entries `versionstamp` is considered the version of the blob.

### `remove()`

Similar to `Deno.Kv.prototype.delete()`, in that it deletes a blob key and value
form the data store. If a blob value isn't stored for the given key, it resolves
in a noop.

### `toBlob()`

A convenience function which takes a string value, and optional media type, and
converts it into a `Blob` which then can be stored via `set()`.

### `toJSON()`

Convert an array buffer, typed array, `Blob` or `File` to a format which can be
stringified into a JSON string.

### `toValue()`

Convert a JSON representation of a blob back into a value. Depending on what
kind of value the JSON represents, either a `Uint8Array`, `Blob` or `File` is
returned.

## Crypto

APIs which allow you to encrypt and decrypt values within a Deno KV datastore.

### `generateKey()`

Generate a random crypto key which can be used with `CryptoKv`. The value
returned will be a hex encoded string. It defaults to 256 bits long. A value of
128, 192, 256 bits can be supplied as an argument to the function to change the
key length.

### `openCryptoKv()`

Like `Deno.openKv()` but returns an instance of `CryptoKv`. `encryptWith`
argument needs to be supplied, which is either a hex encoded string or
`Uint8Array` that is 128, 192, 256 bits in length or an object which conforms to
the `Encryptor` interface. The `path` option works just like `path` option for
`Deno.openKv()`.

### `CryptoKv`

A class which currently provides the ability to set, get and delete encrypted
blob values. If created directly, an instance of `Deno.Kv` needs to be passed to
the constructor along with a value for `encryptWith`.

When a key is supplied,

### `Encryptor`

An interface which specifies two methods of `encrypt()` and `decrypt()` which
will be used for encrypting and decrypting values. This can be used to provide
alternative

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
