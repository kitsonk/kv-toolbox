import {
  assert,
  assertEquals,
  assertStrictEquals,
  concat,
  setup,
  teardown,
} from "./_test_util.ts";

import {
  exportEntries,
  exportToResponse,
  importEntries,
  ImportError,
  LinesTransformStream,
} from "./ndjson.ts";

const decoder = new TextDecoder();
const encoder = new TextEncoder();

Deno.test({
  name: "exportEntries - bytes",
  async fn() {
    const kv = await setup();
    await kv.set(["a"], 100n);
    await kv.set(["b"], new Uint8Array([1, 2, 3]));
    let u8 = new Uint8Array();
    for await (const chunk of exportEntries(kv, { prefix: [] })) {
      u8 = concat([u8, chunk]);
    }
    const actual = decoder.decode(u8);
    assertEquals(actual.split("\n").length, 3);
    assert(
      actual.startsWith(
        `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":`,
      ),
    );
    return teardown();
  },
});

Deno.test({
  name: "exportEntries - text",
  async fn() {
    const kv = await setup();
    await kv.set(["a"], 100n);
    await kv.set(["b"], new Uint8Array([1, 2, 3]));
    const chunks: string[] = [];
    for await (
      const chunk of exportEntries(kv, { prefix: [] }, { text: true })
    ) {
      assert(typeof chunk === "string");
      chunks.push(chunk);
    }
    assertEquals(chunks.length, 2);
    assert(
      chunks[0].startsWith(
        `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":`,
      ),
    );
    return teardown();
  },
});

Deno.test({
  name: "exportToResponse - no filename",
  async fn() {
    const kv = await setup();
    await kv.set(["a"], 100n);
    await kv.set(["b"], new Uint8Array([1, 2, 3]));
    const response = exportToResponse(kv, { prefix: [] });
    assertEquals(response.headers.get("content-type"), "application/x-ndjson");
    assertEquals(response.headers.get("content-disposition"), null);
    let u8 = new Uint8Array();
    for await (const chunk of exportEntries(kv, { prefix: [] })) {
      u8 = concat([u8, chunk]);
    }
    const actual = decoder.decode(u8);
    assertEquals(actual.split("\n").length, 3);
    assert(
      actual.startsWith(
        `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":`,
      ),
    );
    return teardown();
  },
});

Deno.test({
  name: "exportToResponse - filename",
  async fn() {
    const kv = await setup();
    await kv.set(["a"], 100n);
    await kv.set(["b"], new Uint8Array([1, 2, 3]));
    const response = exportToResponse(kv, { prefix: [] }, {
      filename: "export",
    });
    assertEquals(response.headers.get("content-type"), "application/x-ndjson");
    assertEquals(
      response.headers.get("content-disposition"),
      `attachment; filename="export.ndjson"`,
    );
    let u8 = new Uint8Array();
    for await (const chunk of exportEntries(kv, { prefix: [] })) {
      u8 = concat([u8, chunk]);
    }
    const actual = decoder.decode(u8);
    assertEquals(actual.split("\n").length, 3);
    assert(
      actual.startsWith(
        `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":`,
      ),
    );
    return teardown();
  },
});

const fixture =
  `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}
{"key":[{"type":"string","value":"b"}],"value":{"type":"boolean","value":true},"versionstamp":"000000000000000f0000"}
`;

Deno.test({
  name: "LinesTransformStream",
  async fn() {
    const stream = new Blob([fixture]).stream().pipeThrough(
      new LinesTransformStream(),
    );
    const actual: string[] = [];
    for await (const chunk of stream) {
      actual.push(chunk);
    }
    assertEquals(actual, [
      `{"key":[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}`,
      `{"key":[{"type":"string","value":"b"}],"value":{"type":"boolean","value":true},"versionstamp":"000000000000000f0000"}`,
    ]);
  },
});

Deno.test({
  name: "importEntries() - no prefix",
  async fn() {
    const kv = await setup();
    const result = await importEntries(kv, fixture);
    assertEquals(result, { count: 2, skipped: 0, errors: 0 });
    assertEquals((await kv.get(["a"])).value, 100n);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - no overwrite",
  async fn() {
    const kv = await setup();
    await kv.set(["a"], 100);
    const result = await importEntries(kv, fixture);
    assertEquals(result, { count: 2, skipped: 1, errors: 0 });
    assertEquals((await kv.get(["a"])).value, 100);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - overwrite",
  async fn() {
    const kv = await setup();
    await kv.set(["a"], 100);
    const result = await importEntries(kv, fixture, { overwrite: true });
    assertEquals(result, { count: 2, skipped: 0, errors: 0 });
    assertEquals((await kv.get(["a"])).value, 100n);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - data as stream",
  async fn() {
    const kv = await setup();
    const result = await importEntries(kv, new Blob([fixture]).stream());
    assertEquals(result, { count: 2, skipped: 0, errors: 0 });
    assertEquals((await kv.get(["a"])).value, 100n);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - data as Blob",
  async fn() {
    const kv = await setup();
    const result = await importEntries(kv, new Blob([fixture]));
    assertEquals(result, { count: 2, skipped: 0, errors: 0 });
    assertEquals((await kv.get(["a"])).value, 100n);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - data as Uint8Array",
  async fn() {
    const kv = await setup();
    const result = await importEntries(kv, encoder.encode(fixture));
    assertEquals(result, { count: 2, skipped: 0, errors: 0 });
    assertEquals((await kv.get(["a"])).value, 100n);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - data as ArrayBuffer",
  async fn() {
    const kv = await setup();
    const result = await importEntries(kv, encoder.encode(fixture).buffer);
    assertEquals(result, { count: 2, skipped: 0, errors: 0 });
    assertEquals((await kv.get(["a"])).value, 100n);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - onProgress",
  async fn() {
    const kv = await setup();
    const progress: [number, number, number][] = [];
    const result = await importEntries(kv, fixture, {
      onProgress(count, skipped, errors) {
        progress.push([count, skipped, errors]);
      },
    });
    assertEquals(result, { count: 2, skipped: 0, errors: 0 });
    assertEquals(progress, [
      [1, 0, 0],
      [2, 0, 0],
    ]);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - with errors",
  async fn() {
    const kv = await setup();
    const fixture =
      `{key:[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}
{"key":[{"type":"string","value":"b"}],"value":{"type":"boolean","value":true},"versionstamp":"000000000000000f0000"}
`;
    const result = await importEntries(kv, fixture);
    assertEquals(result, { count: 2, skipped: 0, errors: 1 });
    assertEquals((await kv.get(["a"])).value, null);
    assertEquals((await kv.get(["b"])).value, true);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - on error",
  sanitizeResources: false,
  async fn() {
    const kv = await setup();
    const fixture =
      `{key:[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}
{"key":[{"type":"string","value":"b"}],"value":{"type":"boolean","value":true},"versionstamp":"000000000000000f0000"}
`;
    const errors: ImportError[] = [];
    const result = await importEntries(kv, fixture, {
      onError(error) {
        errors.push(error);
      },
    });
    assertEquals(result, { count: 2, skipped: 0, errors: 1 });
    assertEquals((await kv.get(["a"])).value, null);
    assertEquals((await kv.get(["b"])).value, true);
    assertEquals(errors.length, 1);
    assert(errors[0] instanceof ImportError);
    assert(errors[0].cause instanceof SyntaxError);
    assertEquals(errors[0].count, 1);
    assertEquals(errors[0].errors, 1);
    assertEquals(
      errors[0].json,
      `{key:[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}`,
    );
    assertStrictEquals(errors[0].kv, kv);
    assertEquals(errors[0].skipped, 0);
    return teardown();
  },
});

Deno.test({
  name: "importEntries() - error throws",
  sanitizeResources: false,
  async fn() {
    const kv = await setup();
    const fixture =
      `{key:[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}
{"key":[{"type":"string","value":"b"}],"value":{"type":"boolean","value":true},"versionstamp":"000000000000000f0000"}
`;
    let thrown = false;
    try {
      await importEntries(kv, fixture, { throwOnError: true });
    } catch (error) {
      thrown = true;
      assert(error instanceof ImportError);
      assert(error.cause instanceof SyntaxError);
      assertEquals(error.count, 1);
      assertEquals(error.errors, 1);
      assertEquals(
        error.json,
        `{key:[{"type":"string","value":"a"}],"value":{"type":"bigint","value":"100"},"versionstamp":"00000000000000060000"}`,
      );
      assertStrictEquals(error.kv, kv);
      assertEquals(error.skipped, 0);
    }
    assert(thrown);
    return teardown();
  },
});
