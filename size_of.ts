/**
 * An internal module to estimate the byte size of a value.
 *
 * @module
 */

const encoder = new TextEncoder();

function sizeOfString(str: string) {
  return encoder.encode(str).byteLength + 4;
}

function sizeOfError(seen: WeakSet<object>, error: Error) {
  seen.add(error);
  let bytes = error.name.length + sizeOfString(error.message);
  if (error.stack) {
    bytes += sizeOfString(error.stack);
  }
  if (error.cause) {
    bytes += getCalc(seen)(error.cause);
  }
  return bytes - 4;
}

function sizeOfMap(seen: WeakSet<object>, map: Map<unknown, unknown>) {
  seen.add(map);
  let bytes = 0;
  for (const [key, value] of map) {
    bytes += getCalc(seen)(key) - 1;
    bytes += getCalc(seen)(value) - 1;
  }
  return bytes - 1;
}

function sizeOfSet(seen: WeakSet<object>, set: Set<unknown>) {
  seen.add(set);
  let bytes = 0;
  for (const value of set) {
    bytes += getCalc(seen)(value) - 1;
  }
  return bytes;
}

function sizeOfObject(
  seen: WeakSet<object>,
  value: Record<string, unknown>,
) {
  let bytes = 0;
  for (const key of Object.keys(value)) {
    if (typeof value[key] === "object" && value[key] !== null) {
      if (seen.has(value)) {
        continue;
      }
      seen.add(value[key] as object);
    }
    bytes += getCalc(seen)(key);
    try {
      bytes += getCalc(seen)(value[key]);
    } catch (error) {
      if (error instanceof RangeError) {
        bytes = 0;
      }
    }
  }
  return Math.max(bytes + 1, 5);
}

function getCalc(seen: WeakSet<object>): (value: unknown) => number {
  return function calc(value: unknown) {
    switch (typeof value) {
      case "string":
        return sizeOfString(value);
      case "boolean":
        return 3;
      case "number":
        return value < 64
          ? 4
          : value < 8_192
          ? 5
          : value < 1_048_576
          ? 6
          : value < 134_217_728
          ? 7
          : value < 2_147_483_648
          ? 8
          : 11;
      case "bigint":
        return 12;
      case "undefined":
        return 3;
      case "object":
        if (value === null) {
          return 3;
        }
        if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
          return value.byteLength + 9;
        }
        if (Array.isArray(value)) {
          return value.map(getCalc(seen)).reduce(
            (acc, curr) => acc + curr - 1,
            0,
          );
        }
        if (value instanceof Date) {
          return 11;
        }
        if (value instanceof RegExp) {
          return encoder.encode(value.source).byteLength + 6;
        }
        if (value instanceof Error) {
          return sizeOfError(seen, value);
        }
        if (value instanceof Set) {
          return sizeOfSet(seen, value);
        }
        if (value instanceof Map) {
          return sizeOfMap(seen, value);
        }
        if (value instanceof Deno.KvU64) {
          return 12;
        }
        return sizeOfObject(seen, value as Record<string | symbol, unknown>);
      default:
        return 0;
    }
  };
}

/**
 * Estimates the size, in bytes, of the V8 serialized form of the value, which
 * is used to determine the size of messages being stored with Deno KV.
 *
 * This exists when V8 serialize is not available, like when running on Deno
 * Deploy.
 */
export function sizeOf(value: unknown): number {
  return getCalc(new WeakSet())(value);
}
