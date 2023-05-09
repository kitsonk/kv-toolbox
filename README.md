# kv-toolbox

A set of tools for working with Deno KV.

## Blob

A set of APIs for storing arbitrarily sized blobs in Deno KV. Currently Deno KV
has a limit of key values being 64k. The `set()` function breaks down a blob
into chunks and manages sub-keys to store the complete value. The `get()`
function reverses that process.

### `set()`

Similar to `Deno.Kv.prototype.set()`, in that it stores a blob value with an
associated key. In order to deal with the size limitations of values, `set()`
will transparently chunk up the blob into parts that can be handled by Deno KV.

The blob can be a byte `ReadableStream` or array buffer like.

### `get()`

Similar to `Deno.Kv.prototype.get()`, in that it retrieves a blob value based on
the provided key. If a previous blob value has been set with `set()`, it will be
retrieved.

By default the value is resolved as a `Uint8Array` but if the option `stream` is
set to `true`, then a byte `ReadableStream` is provided to read out the blob.

### `remove()`

Similar to `Deno.Kv.prototype.delete()`, in that it deletes a blob key and value
form the data store. If a blob value isn't stored for the given key, it resolves
in a noop.

## Keys

APIs for dealing with Deno KV keys.

### `keys()`

Similar to `Deno.Kv.prototype.list()`, in that is takes a selector, but instead
of returning an async iterator of matched values, it resolves with an array of
matching keys.
