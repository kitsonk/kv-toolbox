/**
 * Utilities for handling [NDJSON](https://github.com/ndjson/ndjson-spec) which
 * is a method for encoding JSON in a way that supports streaming, where each
 * JSON entity is separated with a newline.
 *
 * ## Exporting Deno KV entries
 *
 * {@linkcode exportEntries} works like {@linkcode Deno.Kv} `.list()` but the
 * response is a stream of records encoded as NDJSON. By default it provides
 * the response as a byte stream. Using the `text` option will cause the stream
 * to emit individual records.
 *
 * {@linkcode exportToResponse} also works like {@linkcode Deno.Kv} `.list()`
 * but returns a {@linkcode Response} which can be sent to a client where the
 * body of the response are the entries encoded as NDJSON records. Using the
 * `filename` options will also set the `"Content-Disposition"` header in the
 * response which will indicate to a client to treat the file as a download.
 *
 * ## Decoding NDJSON
 *
 * The {@linkcode LinesTransformStream} takes a byte stream and outputs
 * individual lines which can be used to attempt to parse them as JSON strings.
 *
 * ## Importing Deno KV entries
 *
 * The {@linkcode importEntries} function can be used to import data encoded as
 * NDJSON into a {@linkcode Deno.Kv} store. The data can be in provided in
 * several forms, including byte {@linkcode ReadableStream}, {@linkcode Blob},
 * {@linkcode File}, {@linkcode ArrayBuffer}, typed array or string.
 *
 * @module
 */

import { concat } from "jsr:@std/bytes@~1/concat";

import { entryToJSON, type KvEntryJSON, toKey, toValue } from "./json.ts";

export interface ExportEntriesOptionsJSON extends Deno.KvListOptions {
  /**
   * Determines if the function should close the provided KV store once all the
   * entities are exported. By default, the store won't be closed.
   */
  close?: boolean;
  /**
   * Determines if the chunks of the readable stream will be "raw" JSON entries
   * or already encoded as a byte stream of NDJSON. If `true` they will be
   * individual JSON entries, otherwise a byte stream.
   */
  text: true;
}

export interface ExportEntriesOptionsBytes extends Deno.KvListOptions {
  /**
   * Determines if the function should close the provided KV store once all the
   * entities are exported. By default, the store won't be closed.
   */
  close?: boolean;
  /**
   * Determines if the chunks of the readable stream will be "raw" JSON entries
   * or already encoded as a byte stream of NDJSON. If `true` they will be
   * individual JSON entries, otherwise a byte stream.
   */
  text?: boolean;
}

/**
 * Extends {@linkcode Deno.KvListOptions} with the `json` option.
 */
export type ExportEntriesOptions =
  | ExportEntriesOptionsJSON
  | ExportEntriesOptionsBytes;

/**
 * Options which can be set on {@linkcode exportToResponse}.
 */
export interface ExportToResponseOptions extends Deno.KvListOptions {
  /**
   * Determines if the function should close the provided KV store once all the
   * entities are exported. By default, the store won't be closed.
   */
  close?: boolean;
  /**
   * If provided, the response will include a header that indicates the file is
   * meant to be downloaded (`Content-Disposition`). The extension `.ndjson`
   * will be appended to the filename.
   */
  filename?: string;
}

/**
 * Options which can be set when calling {@linkcode importEntries}.
 */
export interface ImportEntriesOptions {
  /**
   * Determines what happens when a key already exists in the target store for
   * an entry being being import. By default the entry will be skipped. Setting
   * the `overwrite` option to `true` will cause any existing value to be
   * overwritten with the imported value.
   */
  overwrite?: boolean;
  /**
   * An optional callback which occurs when an error is encountered when
   * importing entries. The supplied error will provide details about what was
   * occurring.
   *
   * See {@linkcode ImportError} for more details.
   */
  onError?: (error: ImportError) => void;
  /**
   * An optional callback which occurs every time an entry has been successfully
   * processed, providing an update of the number of entries processed, the
   * number of those that were skipped and the number of those that errored.
   */
  onProgress?: (count: number, skipped: number, errors: number) => void;
  /**
   * The prefix which should be prepended to the front of each entry key when
   * importing. This makes it useful to "namespace" imported data. For example
   * if you were bring in a data set of people, you might supply the
   * {@linkcode Deno.KvKey} of `["person"]`. The imported entry key of `[1]`
   * would then become `["person", 1]`.
   */
  prefix?: Deno.KvKey;
  /**
   * Used to stop the import process. When the signal is aborted, the current
   * import entry will be completed and then the function will return.
   */
  signal?: AbortSignal;
  /**
   * By default, {@linkcode importEntries} will not throw on errors that occur
   * while processing the import data, but just increment the `errors` value
   * and call the `onError()` callback if provided.
   *
   * By setting this to `true`, an {@linkcode ImportError} will be thrown when
   * an error is encountered and terminate the import process.
   */
  throwOnError?: boolean;
}

/**
 * The result returned from calling {@linkcode importEntries}.
 */
export interface ImportEntriesResult {
  /** If set, the import process was aborted prior to completing. */
  aborted?: true;
  /** The number of entries read from the input data. */
  count: number;
  /** The number of entries skipped from the input data. Entries are skipped
   * if a matching entry key is already present in the target, unless the
   * `overwrite` option is set to `true`.
   */
  skipped: number;
  /**
   * The number of entries that errored while processing the data.
   */
  errors: number;
}

interface ImportErrorOptions extends ErrorOptions {
  count: number;
  errors: number;
  json?: string;
  kv: Deno.Kv;
  skipped: number;
}

/**
 * The media type associated with NDJSON.
 */
export const MEDIA_TYPE_NDJSON = "application/x-ndjson";
/**
 * The media type for JSONL which is compatible with NDJSON.
 */
export const MEDIA_TYPE_JSONL = "application/jsonl";
/**
 * The media type for JSON Lines which is compatible with NDJSON.
 */
export const MEDIA_TYPE_JSON_LINES = "application/json-lines";
/**
 * The file extension to use with NDJSON files.
 */
export const EXT_NDJSON = ".ndjson";

const LF = 0x0a;
const CR = 0x0d;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function stripEol(u8: Uint8Array): Uint8Array {
  const length = u8.byteLength;
  if (u8[length - 1] === LF) {
    let drop = 1;
    if (length > 1 && u8[length - 2] === CR) {
      drop = 2;
    }
    return u8.subarray(0, length - drop);
  }
  return u8;
}

/**
 * Like {@linkcode Deno.Kv} `.list()` method, but returns a
 * {@linkcode ReadableStream} where entries are converted to a JSON structure.
 *
 * This is ideal for streaming ndjson as part of a response.
 */
export function exportEntries(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options: ExportEntriesOptionsJSON,
): ReadableStream<string>;
/**
 * Like {@linkcode Deno.Kv} `.list()` method, but returns a
 * {@linkcode ReadableStream} where entries are already converted to their
 * raw byte representation after being encoded as JSON.
 *
 * This is ideal for streaming ndjson as part of a response.
 */
export function exportEntries(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options?: ExportEntriesOptionsBytes,
): ReadableStream<Uint8Array>;
export function exportEntries(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options: ExportEntriesOptions = {},
): ReadableStream<string | Uint8Array> {
  const text = options.text ?? false;
  let cancelled = false;
  return new ReadableStream<string | Uint8Array>({
    async start(controller) {
      try {
        for await (const entry of kv.list(selector, options)) {
          const chunk = entryToJSON(entry);
          controller.enqueue(
            text
              ? `${JSON.stringify(chunk)}\n`
              : encoder.encode(`${JSON.stringify(chunk)}\n`),
          );
          if (cancelled) {
            return;
          }
        }
        if (options.close) {
          kv.close();
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
    cancel(_reason) {
      cancelled = true;
    },
  });
}

/**
 * Like {@linkcode Deno.Kv} `.list()` method, but returns a {@linkcode Response}
 * which will have a body that will be the exported entries that match the
 * selector.
 *
 * The response will contain the appropriate content type and the `filename`
 * option can be used to set the content disposition header so the client
 * understands a file is being downloaded.
 */
export function exportToResponse(
  kv: Deno.Kv,
  selector: Deno.KvListSelector,
  options: ExportToResponseOptions = {},
): Response {
  const body = exportEntries(kv, selector, options);
  const init = {
    headers: {
      "content-type": MEDIA_TYPE_NDJSON,
    } as Record<string, string>,
  } satisfies ResponseInit;
  if (options.filename) {
    init.headers["content-disposition"] =
      `attachment; filename="${options.filename}${EXT_NDJSON}"`;
  }
  return new Response(body, init);
}

export class LinesTransformStream extends TransformStream<Uint8Array, string> {
  #buffer = new Uint8Array(0);
  #pos = 0;

  constructor() {
    super({
      transform: (chunk, controller) => {
        this.#transform(chunk, controller);
      },
      flush: (controller) => {
        const slice = stripEol(this.#buffer.subarray(this.#pos));
        if (slice.length) {
          try {
            controller.enqueue(decoder.decode(slice));
          } catch (error) {
            controller.error(error);
          }
        }
      },
    });
  }

  #readLineBytes(): Uint8Array | null {
    let slice: Uint8Array | null = null;
    const i = this.#buffer.subarray(this.#pos).indexOf(LF);
    if (i >= 0) {
      slice = this.#buffer.subarray(this.#pos, this.#pos + i + 1);
      this.#pos += i + 1;
      return stripEol(slice);
    }
    return null;
  }

  *#lines(): IterableIterator<string | null> {
    while (true) {
      const bytes = this.#readLineBytes();
      if (!bytes) {
        this.#truncate();
        return null;
      }
      yield decoder.decode(bytes);
    }
  }

  #transform(
    chunk: Uint8Array,
    controller: TransformStreamDefaultController<string>,
  ) {
    this.#buffer = concat([this.#buffer, chunk]);
    const iterator = this.#lines();
    while (true) {
      try {
        const result = iterator.next();
        if (result.value) {
          controller.enqueue(result.value);
        }
        if (result.done) {
          break;
        }
      } catch (error) {
        controller.error(error);
      }
    }
  }

  #truncate() {
    this.#buffer = this.#buffer.slice(this.#pos);
    this.#pos = 0;
  }
}

/**
 * An error that can occur when importing records into a {@linkcode Deno.Kv}
 * store. Information associated with the error is available with the `cause`
 * being set to the original error that was thrown.
 */
export class ImportError extends Error {
  #count: number;
  #errors: number;
  #json?: string;
  #kv: Deno.Kv;
  #skipped: number;

  /**
   * The number of entries that had been read from the stream when the
   * error occurred.
   */
  get count(): number {
    return this.#count;
  }
  /**
   * The number of errors in aggregate that had occurred to this point.
   */
  get errors(): number {
    return this.#errors;
  }
  /**
   * If available, the most recent JSON string what had been read from the data.
   */
  get json(): string | undefined {
    return this.#json;
  }
  /**
   * Reference to the {@linkcode Deno.Kv} store that was the target for the
   * import.
   */
  get kv(): Deno.Kv {
    return this.#kv;
  }
  /**
   * The aggregate number of records that had been skipped.
   */
  get skipped(): number {
    return this.#skipped;
  }

  constructor(
    message: string,
    { count, errors, json, kv, skipped, ...options }: ImportErrorOptions,
  ) {
    super(message, options);
    this.#count = count;
    this.#errors = errors;
    this.#json = json;
    this.#kv = kv;
    this.#skipped = skipped;
  }
}

/**
 * Allows NDJSON to be imported in a target {@linkcode Deno.Kv}.
 *
 * The `data` can be in multiple forms, including {@linkcode ReadableStream},
 * {@linkcode Blob}, {@linkcode File}, {@linkcode ArrayBuffer}, typed array, or
 * string.
 */
export async function importEntries(
  kv: Deno.Kv,
  data:
    | ReadableStream<Uint8Array>
    | Blob
    | ArrayBufferView
    | ArrayBuffer
    | string,
  options: ImportEntriesOptions = {},
): Promise<ImportEntriesResult> {
  const {
    overwrite = false,
    prefix = [],
    onError,
    onProgress,
    signal,
    throwOnError,
  } = options;
  let stream: ReadableStream<string>;
  const transformer = new LinesTransformStream();
  if (data instanceof ReadableStream) {
    stream = data.pipeThrough(transformer);
  } else if (data instanceof Blob) {
    stream = data.stream().pipeThrough(transformer);
  } else {
    stream = new Blob([data]).stream().pipeThrough(transformer);
  }
  const reader = stream.getReader();
  let count = 0;
  let errors = 0;
  let skipped = 0;
  while (true) {
    let result: ReadableStreamReadResult<string> | undefined = undefined;
    try {
      result = await reader.read();
      if (result.value) {
        count++;
        const entry: KvEntryJSON = JSON.parse(result.value);
        const { key, value } = entry;
        const entryKey = prefix.length
          ? [...prefix, ...toKey(key)]
          : toKey(key);
        if (!overwrite) {
          const { versionstamp } = await kv.get(entryKey);
          if (versionstamp) {
            skipped++;
            continue;
          }
        }
        await kv.set(entryKey, toValue(value));
        onProgress?.(count, skipped, errors);
      }
      if (result.done) {
        break;
      }
      if (signal?.aborted) {
        reader.releaseLock();
        return { aborted: true, count, skipped, errors };
      }
    } catch (cause) {
      errors++;
      if (onError || throwOnError) {
        const error = new ImportError(
          cause instanceof Error ? cause.message : "An import error occurred.",
          { cause, json: result?.value, count, kv, skipped, errors },
        );
        onError?.(error);
        if (throwOnError) {
          reader.releaseLock();
          throw error;
        }
      }
    }
  }
  reader.releaseLock();
  return { count, skipped, errors };
}
