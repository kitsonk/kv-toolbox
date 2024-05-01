import { Serializer } from "jsr:@denostack/superserial@0.3.5";
import { serialize } from "node:v8";
import { sizeOf } from "./size_of.ts";

const fixture = {
  "🦕": /abcd/i,
  nested: { a: new Set([{}, 2, 3]) },
  buffer: [new Uint8Array(65_000), new Uint8Array(65_000)],
  long_string: "a".repeat(2_000),
};

Deno.bench({
  name: "serialize().byteLength",
  fn() {
    serialize(fixture).byteLength;
  },
});

Deno.bench({
  name: "sizeOf()",
  fn() {
    sizeOf(fixture);
  },
});

const serializer = new Serializer();
const encoder = new TextEncoder();

Deno.bench({
  name: "superserial serializer.serialize()",
  fn() {
    encoder.encode(serializer.serialize(fixture)).byteLength;
  },
});
