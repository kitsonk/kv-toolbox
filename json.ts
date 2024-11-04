/**
 * Utilities for handling Deno KV entries, keys, and values as structures
 * which can be serialized and deserialized to JSON.
 *
 * This is useful when communicating entries and values outside of the runtime
 * environment.
 *
 * **Converting to JSON**
 *
 * ```ts
 * import { entryMaybeToJSON } from "jsr:@kitsonk/kv-toolbox/json";
 *
 * const kv = await Deno.openKv();
 * const entryMaybe = await kv.get(["a"]);
 *
 * // `json` is now an object which can be safely converted to a JSON string
 * const json = entryMaybeToJSON(entryMaybe);
 * kv.close();
 * ```
 *
 * **Converting a JSON value**
 *
 * ```ts
 * import { toValue } from "jsr:@kitsonk/kv-toolbox/json";
 *
 * // `json` represents a `Uint8Array` with the bytes of [1, 2, 3]
 * const json = { type: "Uint8Array", value: "AQID" };
 *
 * const kv = await Deno.openKv();
 * await kv.set(["a"], toValue(json));
 * kv.close();
 * ```
 *
 * @module
 */

import {
  decodeBase64Url,
  encodeBase64Url,
} from "jsr:@std/encoding@~1/base64url";

// Deno KV Key types

/**
 * A JSON representation of a {@linkcode bigint} Deno KV key part. The value
 * is a string representation of the integer, for example `100n` would be:
 *
 * ```json
 * { "type": "bigint", "value": "100" }
 * ```
 */
export interface KvBigIntJSON {
  type: "bigint";
  value: string;
}

/**
 * A JSON representation of a {@linkcode boolean} Deno KV key part. The value
 * is the boolean value, for example `true` would be:
 *
 * ```json
 * { "type": "boolean", "value": true }
 * ```
 */
export interface KvBooleanJSON {
  type: "boolean";
  value: boolean;
}

/**
 * A JSON representation of a {@linkcode number} Deno KV key part. The value
 * is the number value, for example `100` would be:
 *
 * ```json
 * { "type": "number", "value": 100 }
 * ```
 */
export interface KvNumberJSON {
  type: "number";
  value: number | "Infinity" | "-Infinity" | "NaN";
}

/**
 * A JSON representation of a {@linkcode string} Deno KV key part. The value is
 * the string value, for example `"value"` would be:
 *
 * ```json
 * { "type": "string", "value": "value" }
 * ```
 */
export interface KvStringJSON {
  type: "string";
  value: string;
}

/**
 * A JSON representation of a {@linkcode Uint8Array} Deno KV key part. The value
 * is a URL safe base64 encoded value, for example an array with the values of
 * `[ 1, 2, 3 ]` would be:
 *
 * ```json
 * { "type": "Uint8Array", "value": "AQID" }
 * ```
 *
 * While Deno KV accepts anything that is array view like as a key part, when
 * the value is read as part of an entry, it is always represented as a
 * `Uint8Array`.
 */
export interface KvUint8ArrayJSON {
  type: "Uint8Array";
  value: string;
}

/**
 * JSON representations of {@linkcode Deno.KvKeyPart}. This represents each key
 * part type that is supported by Deno KV.
 */
export type KvKeyPartJSON =
  | KvBigIntJSON
  | KvBooleanJSON
  | KvNumberJSON
  | KvStringJSON
  | KvUint8ArrayJSON;

/**
 * A JSON representation of a {@linkcode Deno.KvKey}, which is an array of
 * {@linkcode KvKeyPartJSON} items.
 */
export type KvKeyJSON = KvKeyPartJSON[];

// Deno KV Value types

/**
 * A representation of an {@linkcode ArrayBuffer} Deno KV value. The value is
 * the bytes of the array buffer encoded as a URL safe base64 string, for
 * example an array buffer with the byte values of `[ 1, 2, 3 ]` would be:
 *
 * ```json
 * { "type": "ArrayBuffer", "value": "AQID" }
 * ```
 */
export interface KvArrayBufferJSON {
  type: "ArrayBuffer";
  byteLength?: number;
  value: string;
}

/**
 * A representation of an {@linkcode Array} Deno KV value. The value is the
 * JSON serialized version of the elements of the array.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export interface KvLegacyArrayJSON<T = unknown> {
  type: "Array";
  value: T[];
}

/**
 * A representation of an {@linkcode Array} Deno KV value. The value is the
 * JSON serialized version of the elements of the array.
 */
export interface KvArrayJSON {
  type: "json_array";
  value: KvValueJSON[];
}

/**
 * A representation of an {@linkcode DataView} Deno KV value. The value is
 * the bytes of the buffer encoded as a URL safe base64 string, for example a
 * data view with the byte values of `[ 1, 2, 3 ]` would be:
 *
 * ```json
 * { "type": "DataView", "value": "AQID" }
 * ```
 */
export interface KvDataViewJSON {
  type: "DataView";
  byteLength?: number;
  value: string;
}

/**
 * A representation of a {@linkcode Date} Deno KV value. The value is the
 * [ISO string representation](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString)
 * of the date.
 */
export interface KvDateJSON {
  type: "Date";
  value: string;
}

/**
 * Error instances which are
 * [cloneable](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Structured_clone_algorithm#error_types)
 * and therefore can be stored in a Deno KV store.
 *
 * This type is used to allow type inference when deserializing from JSON.
 */
interface CloneableErrors {
  Error: Error;
  EvalError: EvalError;
  RangeError: RangeError;
  ReferenceError: ReferenceError;
  SyntaxError: SyntaxError;
  TypeError: TypeError;
  URIError: URIError;
}

/**
 * The keys of {@linkcode CloneableErrors} which is used for type inference
 * when deserializing from JSON.
 */
type CloneableErrorTypes = keyof CloneableErrors;

/**
 * A representation of {@linkcode Error}s that can be stored as Deno KV values.
 * The value is set to a serialized version of the value. Instances that are
 * not one of the specified types, but inherit from `Error` will be serialized
 * as `Error`. */
export interface KvErrorJSON<
  ErrorType extends CloneableErrorTypes = CloneableErrorTypes,
> {
  type: ErrorType;
  value: {
    message: string;
    cause?: KvValueJSON;
    stack?: string;
  };
}

/**
 * A representation of a {@linkcode Deno.KvU64} value. The value is the string
 * representation of the unsigned integer.
 */
export interface KvKvU64JSON {
  type: "KvU64";
  value: string;
}

/**
 * A representation of a {@linkcode Map} Deno KV value. The value is an array
 * of map entries where is map entry is a tuple of a JSON serialized key and
 * value.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export interface KvLegacyMapJSON<K = unknown, V = unknown> {
  type: "Map";
  value: [key: K, value: V][];
}

/**
 * A representation of a {@linkcode Map} Deno KV value. The value is an array
 * of map entries where is map entry is a tuple of a JSON serialized key and
 * value.
 */
export interface KvMapJSON {
  type: "json_map";
  value: [key: KvValueJSON, value: KvValueJSON][];
}

/**
 * A representation of a {@linkcode null} Deno KV value. The value is `null`.
 */
export interface KvNullJSON {
  type: "null";
  value: null;
}

/**
 * A representation of a {@linkcode object} Deno KV value. The value is a JSON
 * serialized version of the value.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export interface KvLegacyObjectJSON<T = unknown> {
  type: "object";
  value: T;
}

/**
 * A representation of a {@linkcode object} Deno KV value. The value is a JSON
 * serialized version of the value.
 */
export interface KvObjectJSON {
  type: "json_object";
  value: { [key: string]: KvValueJSON };
}

/**
 * A representation of a {@linkcode RegExp} Deno KV value. The value is a string
 * representation of the regular expression value.
 */
export interface KvRegExpJSON {
  type: "RegExp";
  value: string;
}

/**
 * A representation of a {@linkcode Set} Deno KV value. The value is an array
 * of JSON serialized entries.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export interface KvLegacySetJSON<T = unknown> {
  type: "Set";
  value: T[];
}

export interface KvSetJSON {
  type: "json_set";
  value: KvValueJSON[];
}

/** Used internally to identify a typed array. */
type TypedArray =
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array;

/**
 * Used internally to be able to map the name of the typed array to its instance
 * type.
 */
interface TypedArrayMap {
  Int8Array: Int8Array;
  Uint8Array: Uint8Array;
  Uint8ClampedArray: Uint8ClampedArray;
  Int16Array: Int16Array;
  Uint16Array: Uint16Array;
  Int32Array: Int32Array;
  Uint32Array: Uint32Array;
  Float32Array: Float32Array;
  Float64Array: Float64Array;
  BigInt64Array: BigInt64Array;
  BigUint64Array: BigUint64Array;
}

/** Used internally. The string literal types of the names of the type. */
type TypedArrayTypes = keyof TypedArrayMap;

/**
 * A representation of a typed array Deno KV value. The value is a URL safe
 * base64 encoded string which represents the individual bytes of the array.
 */
export interface KvTypedArrayJSON<
  ArrayType extends TypedArrayTypes = TypedArrayTypes,
> {
  type: ArrayType;
  byteLength?: number | undefined;
  value: string;
}

/**
 * A representation of a {@linkcode undefined} Deno KV value. The value is
 * undefined, and therefore elided when serialized. Therefore there is only one
 * form of this entity:
 *
 * ```json
 * { "type": "undefined" }
 * ```
 */
export interface KvUndefinedJSON {
  type: "undefined";
}

/**
 * JSON representations of {@linkcode Deno.Kv} values, where the value types are
 * exhaustive of what Deno KV supports and are allowed via
 * [structured cloning](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Structured_clone_algorithm).
 */
export type KvValueJSON =
  | KvArrayBufferJSON
  | KvArrayJSON
  | KvBigIntJSON
  | KvBooleanJSON
  | KvDataViewJSON
  | KvDateJSON
  | KvErrorJSON
  | KvKvU64JSON
  | KvMapJSON
  | KvNullJSON
  | KvNumberJSON
  | KvObjectJSON
  | KvRegExpJSON
  | KvSetJSON
  | KvStringJSON
  | KvTypedArrayJSON
  | KvUndefinedJSON
  | KvLegacyArrayJSON
  | KvLegacyMapJSON
  | KvLegacyObjectJSON
  | KvLegacySetJSON;

// Deno KV Entry types

/**
 * A representation of a {@linkcode Deno.KvEntry} where the key and value are
 * encoded in a JSON serializable format.
 */
export interface KvEntryJSON {
  key: KvKeyJSON;
  value: KvValueJSON;
  versionstamp: string;
}

/**
 * A representation of a {@linkcode Deno.KvEntryMaybe} where the key and value
 * are encoded in a JSON serializable format.
 */
export type KvEntryMaybeJSON = KvEntryJSON | {
  key: KvKeyJSON;
  value: null;
  versionstamp: null;
};

// Serializing to JSON

/** Internal function to serialize various classes of errors to JSON. */
function errorToJSON(error: Error): KvErrorJSON {
  const { message, stack, cause } = error;
  const value: KvErrorJSON["value"] = { message };
  if (cause) {
    value.cause = valueToJSON(cause);
  }
  if (stack) {
    value.stack = stack;
  }
  if (error instanceof EvalError) {
    return { type: "EvalError", value };
  }
  if (error instanceof RangeError) {
    return { type: "RangeError", value };
  }
  if (error instanceof ReferenceError) {
    return { type: "ReferenceError", value };
  }
  if (error instanceof SyntaxError) {
    return { type: "SyntaxError", value };
  }
  if (error instanceof TypeError) {
    return { type: "TypeError", value };
  }
  if (error instanceof URIError) {
    return { type: "URIError", value };
  }
  return { type: "Error", value };
}

/** Internal function to serialize various typed arrays to JSON. */
function typedArrayToJSON(typedArray: ArrayBufferView): KvTypedArrayJSON {
  const value = encodeBase64Url(typedArray.buffer);
  const byteLength = typedArray.byteLength;
  if (typedArray instanceof Int8Array) {
    return { type: "Int8Array", byteLength, value };
  }
  if (typedArray instanceof Uint8Array) {
    return { type: "Uint8Array", byteLength, value };
  }
  if (typedArray instanceof Uint8ClampedArray) {
    return { type: "Uint8ClampedArray", byteLength, value };
  }
  if (typedArray instanceof Int16Array) {
    return { type: "Int16Array", byteLength, value };
  }
  if (typedArray instanceof Uint16Array) {
    return { type: "Uint16Array", byteLength, value };
  }
  if (typedArray instanceof Int32Array) {
    return { type: "Int32Array", byteLength, value };
  }
  if (typedArray instanceof Uint32Array) {
    return { type: "Uint32Array", byteLength, value };
  }
  if (typedArray instanceof Float32Array) {
    return { type: "Float32Array", byteLength, value };
  }
  if (typedArray instanceof Float64Array) {
    return { type: "Float64Array", byteLength, value };
  }
  if (typedArray instanceof BigInt64Array) {
    return { type: "BigInt64Array", byteLength, value };
  }
  if (typedArray instanceof BigUint64Array) {
    return { type: "BigUint64Array", byteLength, value };
  }
  throw TypeError("Unexpected typed array type, could not serialize.");
}

/** Internal function to encode an object. */
function encodeObject(object: object): { [key: string]: KvValueJSON } {
  const result: { [key: string]: KvValueJSON } = {};
  for (const [key, value] of Object.entries(object)) {
    result[key] = valueToJSON(value);
  }
  return result;
}

/** Internal function to decode an object. */
function decodeObject(json: { [key: string]: KvValueJSON }): object {
  const result: { [key: string]: unknown } = {};
  for (const [key, value] of Object.entries(json)) {
    result[key] = toValue(value);
  }
  return result;
}

/** Serialize a {@linkcode Deno.KvKeyPart} to JSON. */
export function keyPartToJSON(value: Deno.KvKeyPart): KvKeyPartJSON {
  switch (typeof value) {
    case "bigint":
      return { type: "bigint", value: String(value) };
    case "boolean":
      return { type: "boolean", value };
    case "number":
      if (Number.isNaN(value)) {
        return { type: "number", value: "NaN" };
      } else if (value === Infinity) {
        return { type: "number", value: "Infinity" };
      } else if (value === -Infinity) {
        return { type: "number", value: "-Infinity" };
      }
      return { type: "number", value };
    case "object":
      if (value instanceof Uint8Array) {
        return { type: "Uint8Array", value: encodeBase64Url(value) };
      }
      break;
    case "string":
      return { type: "string", value };
  }
  throw new TypeError("Unable to serialize value.");
}

/** Serialize a {@linkcode Deno.KvKey} to JSON. */
export function keyToJSON(value: Deno.KvKey): KvKeyJSON {
  return value.map(keyPartToJSON);
}

/** Serialize an array value to JSON. */
export function valueToJSON(value: unknown[]): KvArrayJSON;
/** Serialize a bigint value to JSON. */
export function valueToJSON(value: bigint): KvBigIntJSON;
/** Serialize a boolean value to JSON. */
export function valueToJSON(value: boolean): KvBooleanJSON;
/** Serialize a {@linkcode Date} value to JSON. */
export function valueToJSON(value: Date): KvDateJSON;
/** Serialize an error value to JSON. */
export function valueToJSON<ErrorType extends CloneableErrorTypes>(
  value: Error,
): KvErrorJSON<ErrorType>;
/** Serialize a {@linkcode Deno.KvU64} value to JSON. */
export function valueToJSON(value: Deno.KvU64): KvKvU64JSON;
/** Serialize a {@linkcode Map} value to JSON. */
export function valueToJSON(value: Map<unknown, unknown>): KvMapJSON;
/** Serialize a `null` value to JSON. */
export function valueToJSON(value: null): KvNullJSON;
/** Serialize a number value to JSON. */
export function valueToJSON(value: number): KvNumberJSON;
/** Serialize a {@linkcode RegExp} value to JSON. */
export function valueToJSON(value: RegExp): KvRegExpJSON;
/** Serialize a {@linkcode Set} value to JSON. */
export function valueToJSON(value: Set<unknown>): KvSetJSON;
/** Serialize a string value to JSON. */
export function valueToJSON(value: string): KvStringJSON;
/** Serialize a typed array value to JSON. */
export function valueToJSON<TA extends TypedArray>(
  value: TA,
): KvTypedArrayJSON<TA[typeof Symbol.toStringTag]>;
/** Serialize an {@linkcode ArrayBuffer} value to JSON. */
export function valueToJSON(value: ArrayBufferLike): KvArrayBufferJSON;
/** Serialize an {@linkcode DataView} value to JSON. */
export function valueToJSON(value: DataView): KvDataViewJSON;
/** Serialize a `undefined` value to JSON. */
export function valueToJSON(value: undefined): KvUndefinedJSON;
/** Serialize a object value to JSON. */
export function valueToJSON(value: object): KvObjectJSON;
/** Serialize a value to JSON. */
export function valueToJSON(value: unknown): KvValueJSON;
export function valueToJSON(value: unknown): KvValueJSON {
  switch (typeof value) {
    case "bigint":
    case "boolean":
    case "number":
    case "string":
      return keyPartToJSON(value);
    case "undefined":
      return { type: "undefined" };
    case "object":
      if (Array.isArray(value)) {
        return { type: "json_array", value: value.map(valueToJSON) };
      }
      if (value instanceof DataView) {
        return {
          type: "DataView",
          byteLength: value.byteLength,
          value: encodeBase64Url(value.buffer),
        };
      }
      if (ArrayBuffer.isView(value)) {
        return typedArrayToJSON(value);
      }
      if (value instanceof ArrayBuffer) {
        return {
          type: "ArrayBuffer",
          byteLength: value.byteLength,
          value: encodeBase64Url(value),
        };
      }
      if (value instanceof Date) {
        return { type: "Date", value: value.toJSON() };
      }
      if ("Deno" in globalThis && value instanceof Deno.KvU64) {
        return { type: "KvU64", value: String(value) };
      }
      if (value instanceof Error) {
        return errorToJSON(value);
      }
      if (value instanceof Map) {
        return {
          type: "json_map",
          value: [
            ...value.entries().map((
              [key, value],
            ) => [valueToJSON(key), valueToJSON(value)]),
          ] as [KvValueJSON, KvValueJSON][],
        };
      }
      if (value === null) {
        return { type: "null", value };
      }
      if (value instanceof RegExp) {
        return { type: "RegExp", value: String(value) };
      }
      if (value instanceof Set) {
        return { type: "json_set", value: [...value].map(valueToJSON) };
      }
      return { type: "json_object", value: encodeObject(value) };
    default:
      throw new TypeError("Unexpected value type, unable to serialize.");
  }
}

/** Serialize a {@linkcode Deno.KvEntry} to JSON. */
export function entryToJSON(
  { key, value, versionstamp }: Deno.KvEntry<unknown>,
): KvEntryJSON {
  return {
    key: key.map(keyPartToJSON),
    value: valueToJSON(value),
    versionstamp,
  };
}

/** Serialize a {@linkcode Deno.KvEntryMaybe} to JSON. */
export function entryMaybeToJSON(
  { key, value, versionstamp }: Deno.KvEntryMaybe<unknown>,
): KvEntryMaybeJSON {
  return {
    key: key.map(keyPartToJSON),
    value: value === null && versionstamp === null ? null : valueToJSON(value),
    versionstamp,
  } as KvEntryMaybeJSON;
}

// Deserializing from JSON

function toError(
  { type, value: { message, stack, cause } }: KvErrorJSON,
): Error {
  let error: Error;
  const options = cause ? { cause: toValue(cause) } : undefined;
  switch (type) {
    case "EvalError":
      error = new EvalError(message, options);
      break;
    case "RangeError":
      error = new RangeError(message, options);
      break;
    case "ReferenceError":
      error = new ReferenceError(message, options);
      break;
    case "SyntaxError":
      error = new SyntaxError(message, options);
      break;
    case "TypeError":
      error = new TypeError(message, options);
      break;
    case "URIError":
      error = new URIError(message, options);
      break;
    default:
      error = new Error(message, options);
  }
  if (stack) {
    Object.defineProperty(error, "stack", {
      value: stack,
      writable: false,
      enumerable: false,
      configurable: true,
    });
  }
  return error;
}

/** Internal function to deserialize typed arrays. */
function toTypedArray(json: KvTypedArrayJSON): ArrayBufferView {
  const u8 = decodeBase64Url(json.value);
  switch (json.type) {
    case "BigInt64Array":
      return new BigInt64Array(u8.buffer);
    case "BigUint64Array":
      return new BigUint64Array(u8.buffer);
    case "Float32Array":
      return new Float32Array(u8.buffer);
    case "Float64Array":
      return new Float64Array(u8.buffer);
    case "Int16Array":
      return new Int16Array(u8.buffer);
    case "Int32Array":
      return new Int32Array(u8.buffer);
    case "Int8Array":
      return new Int8Array(u8.buffer);
    case "Uint16Array":
      return new Uint16Array(u8.buffer);
    case "Uint32Array":
      return new Uint32Array(u8.buffer);
    case "Uint8Array":
      return u8;
    case "Uint8ClampedArray":
      return new Uint8ClampedArray(u8.buffer);
    default:
      // deno-lint-ignore no-explicit-any
      throw new TypeError(`Unexpected value type: "${(json as any).type}".`);
  }
}

/** Deserialize {@linkcode KvBigIntJSON} to a bigint. */
export function toKeyPart(json: KvBigIntJSON): bigint;
/** Deserialize {@linkcode KvBooleanJSON} to a boolean. */
export function toKeyPart(json: KvBooleanJSON): boolean;
/** Deserialize {@linkcode KvNumberJSON} to a number. */
export function toKeyPart(json: KvNumberJSON): number;
/** Deserialize {@linkcode KvStringJSON} to a string. */
export function toKeyPart(json: KvStringJSON): string;
/** Deserialize {@linkcode KvUint8ArrayJSON} to a {@linkcode Uint8Array}. */
export function toKeyPart(json: KvUint8ArrayJSON): Uint8Array;
/** Deserialize {@linkcode KvKeyPartJSON} to a {@linkcode Deno.KvKeyPart}. */
export function toKeyPart(json: KvKeyPartJSON): Deno.KvKeyPart;
export function toKeyPart(json: KvKeyPartJSON): Deno.KvKeyPart {
  switch (json.type) {
    case "Uint8Array":
      return decodeBase64Url(json.value);
    case "bigint":
      return BigInt(json.value);
    case "boolean":
    case "string":
      return json.value;
    case "number":
      if (json.value === "Infinity") {
        return Infinity;
      }
      if (json.value === "-Infinity") {
        return -Infinity;
      }
      if (json.value === "NaN") {
        return NaN;
      }
      return json.value;
    default:
      // deno-lint-ignore no-explicit-any
      throw new TypeError(`Unexpected key part type: "${(json as any).type}".`);
  }
}

/** Deserialize {@linkcode KvKeyJSON} to a {@linkcode Deno.KvKey}. */
export function toKey(json: KvKeyJSON): Deno.KvKey {
  return json.map(toKeyPart);
}

/**
 * Deserialize {@linkcode KvArrayBufferJSON} to an {@linkcode ArrayBuffer} which
 * can be stored in a Deno KV store.
 */
export function toValue(json: KvArrayBufferJSON): ArrayBuffer;
/**
 * Deserialize {@linkcode KvArrayJSON} to an array which can be stored in a Deno
 * KV store.
 */
export function toValue(json: KvArrayJSON): unknown[];
/**
 * Deserialize {@linkcode KvLegacyArrayJSON} to an array which can be stored in a Deno
 * KV store.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export function toValue<T>(json: KvLegacyArrayJSON<T>): T[];
/**
 * Deserialize {@linkcode KvBigIntJSON} to a bigint which can be stored in a
 * Deno KV store.
 */
export function toValue(json: KvBigIntJSON): bigint;
/**
 * Deserialize {@linkcode KvBooleanJSON} to a boolean which can be stored in a
 * Deno KV store.
 */
export function toValue(json: KvBooleanJSON): boolean;
/**
 * Deserialize {@linkcode KvDataViewJSON} to a {@linkcode DataView} which can
 * be stored in a Deno KV store.
 */
export function toValue(json: KvDataViewJSON): DataView;
/**
 * Deserialize {@linkcode KvDateJSON} to a {@linkcode Date} which can be stored
 * in a Deno KV store.
 */
export function toValue(json: KvDateJSON): Date;
/**
 * Deserialize {@linkcode KvErrorJSON} to an error value which can be stored in
 * a Deno KV store.
 */
export function toValue<ErrorType extends CloneableErrorTypes>(
  json: KvErrorJSON<ErrorType>,
): CloneableErrors[ErrorType];
/**
 * Deserialize {@linkcode KvKvU64JSON} to a {@linkcode Deno.KvU64} which can be
 * stored in a Deno KV store.
 */
export function toValue(json: KvKvU64JSON): Deno.KvU64;
/**
 * Deserialize {@linkcode KvMapJSON} to a {@linkcode Map} which can be stored in
 * a Deno KV store.
 */
export function toValue(json: KvMapJSON): Map<unknown, unknown>;
/**
 * Deserialize {@linkcode KvLegacyMapJSON} to a {@linkcode Map} which can be stored in
 * a Deno KV store.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export function toValue<K, V>(json: KvLegacyMapJSON<K, V>): Map<K, V>;
/**
 * Deserialize {@linkcode KvNullJSON} to a `null` which can be stored in a Deno
 * KV store.
 */
export function toValue(json: KvNullJSON): null;
/**
 * Deserialize {@linkcode KvNumberJSON} to a number which can be stored in a
 * Deno KV store.
 */
export function toValue(json: KvNumberJSON): number;
/**
 * Deserialize {@linkcode KvObjectJSON} to a value which can be stored in a Deno
 * KV store.
 */
export function toValue(json: KvObjectJSON): Record<string, unknown>;
/**
 * Deserialize {@linkcode KvLegacyObjectJSON} to a value which can be stored in
 * a Deno KV store.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export function toValue<T>(json: KvLegacyObjectJSON<T>): T;
/**
 * Deserialize {@linkcode KvRegExpJSON} to a {@linkcode RegExp} which can be
 * stored in a Deno KV store.
 */
export function toValue(json: KvRegExpJSON): RegExp;
/**
 * Deserialize {@linkcode KvSetJSON} to a {@linkcode Set} which can be stored in
 * a Deno KV store.
 */
export function toValue(json: KvSetJSON): Set<unknown>;
/**
 * Deserialize {@linkcode KvLegacySetJSON} to a {@linkcode Set} which can be
 * stored in a Deno KV store.
 *
 * @deprecated This is a legacy representation and is only retained for
 * compatibility with older versions of the library.
 */
export function toValue<T>(json: KvLegacySetJSON<T>): Set<T>;
/**
 * Deserialize {@linkcode KvStringJSON} to a string which can be stored in a
 * Deno KV store.
 */
export function toValue(json: KvStringJSON): string;
/**
 * Deserialize {@linkcode KvTypedArrayJSON} to a typed array which can be stored
 * in a Deno KV store.
 */
export function toValue<ArrayType extends TypedArrayTypes>(
  json: KvTypedArrayJSON<ArrayType>,
): TypedArrayMap[ArrayType];
/**
 * Deserialize {@linkcode KvUndefinedJSON} to `undefined` which can be stored in
 * a Deno KV store.
 */
export function toValue(json: KvUndefinedJSON): undefined;
/**
 * Deserialize {@linkcode KvValueJSON} to a value which can be stored in a Deno
 * KV store.
 */
export function toValue(json: KvValueJSON): unknown;
export function toValue(json: KvValueJSON): unknown {
  switch (json.type) {
    case "json_array":
      return json.value.map(toValue);
    case "json_map":
      return new Map(json.value.map((
        [key, value]: [KvValueJSON, KvValueJSON],
      ) => [toValue(key), toValue(value)]) as [unknown, unknown][]);
    case "json_object":
      return decodeObject(json.value);
    case "json_set":
      return new Set(json.value.map(toValue));
    case "Array":
    case "null":
    case "object":
      return json.value;
    case "ArrayBuffer":
      return decodeBase64Url(json.value).buffer;
    case "BigInt64Array":
    case "BigUint64Array":
    case "Float32Array":
    case "Float64Array":
    case "Int16Array":
    case "Int32Array":
    case "Int8Array":
    case "Uint16Array":
    case "Uint32Array":
    case "Uint8Array":
    case "Uint8ClampedArray":
      return toTypedArray(json);
    case "DataView":
      return new DataView(decodeBase64Url(json.value).buffer);
    case "Date":
      return new Date(json.value);
    case "Error":
    case "EvalError":
    case "RangeError":
    case "ReferenceError":
    case "SyntaxError":
    case "TypeError":
    case "URIError":
      return toError(json);
    case "KvU64":
      return new Deno.KvU64(BigInt(json.value));
    case "Map":
      return new Map(json.value);
    case "RegExp": {
      const parts = json.value.split("/");
      const flags = parts.pop();
      const [, ...pattern] = parts;
      return new RegExp(pattern.join("/"), flags);
    }
    case "Set":
      return new Set(json.value);
    case "bigint":
    case "boolean":
    case "number":
    case "string":
      return toKeyPart(json);
    case "undefined":
      return undefined;
    default:
      // deno-lint-ignore no-explicit-any
      throw new TypeError(`Unexpected value type: "${(json as any).type}"`);
  }
}

/** Deserialize a {@linkcode KvEntryJSON} to a {@linkcode Deno.KvEntry}. */
export function toEntry<T>(
  { key, value, versionstamp }: KvEntryJSON,
): Deno.KvEntry<T> {
  return {
    key: key.map(toKeyPart),
    value: toValue(value),
    versionstamp,
  } as Deno.KvEntry<T>;
}

/**
 * Deserialize a {@linkcode KvEntryMaybeJSON} to a
 * {@linkcode Deno.KvEntryMaybe}.
 */
export function toEntryMaybe<T>(
  { key, value, versionstamp }: KvEntryMaybeJSON,
): Deno.KvEntryMaybe<T> {
  return {
    key: key.map(toKeyPart),
    value: value === null ? null : toValue(value),
    versionstamp,
  } as Deno.KvEntryMaybe<T>;
}
