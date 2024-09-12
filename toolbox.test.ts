import {
  assert,
  assertEquals,
  cleanup,
  getPath,
  timingSafeEqual,
} from "./_test_util.ts";
import { generateKey } from "./crypto.ts";

import { openKvToolbox } from "./toolbox.ts";

Deno.test({
  name: "kvToolbox - open and close",
  async fn() {
    const path = await getPath();
    const kv = await openKvToolbox({ path });
    kv.close();
    return cleanup();
  },
});

Deno.test({
  name: "kvToolbox - get and set functionality",
  async fn() {
    const path = await getPath();
    const kv = await openKvToolbox({ path });
    const result = await kv.set(["key"], "value");
    assert(result.ok);
    const maybeEntry = await kv.get(["key"]);
    assert(maybeEntry.versionstamp);
    assertEquals(maybeEntry.value, "value");
    kv.close();
    return cleanup();
  },
});

Deno.test({
  name: "kvToolbox - .keys()",
  async fn() {
    const path = await getPath();
    const kv = await openKvToolbox({ path });
    const res = await kv.atomic()
      .set(["a"], "a")
      .set(["a", "b"], "b")
      .set(["a", "b", "c"], "c")
      .set(["a", "d", "f", "g"], "g")
      .set(["a", "h"], "h")
      .set(["e"], "e")
      .commit();
    assert(res[0].ok);

    const actual = await kv.tree();

    assertEquals(actual, {
      children: [
        {
          part: "a",
          hasValue: true,
          children: [
            {
              part: "b",
              hasValue: true,
              children: [{ part: "c", hasValue: true }],
            },
            {
              part: "d",
              children: [{
                part: "f",
                children: [{ part: "g", hasValue: true }],
              }],
            },
            { part: "h", hasValue: true },
          ],
        },
        { part: "e", hasValue: true },
      ],
    });
    kv.close();

    return cleanup();
  },
});

Deno.test({
  name: "kvToolbox - open and close with encryption",
  async fn() {
    const path = await getPath();
    const key = generateKey();
    const kv = await openKvToolbox({ path, encryptWith: key });
    kv.close();
    return cleanup();
  },
});

Deno.test({
  name: "kvToolbox - encrypt/decrypt blob - Uint8Array",
  async fn() {
    const path = await getPath();
    const encryptWith = generateKey();
    const kv = await openKvToolbox({ path, encryptWith });
    const value = globalThis.crypto.getRandomValues(new Uint8Array(65_536));
    const res = await kv.setBlob(["example"], value);
    assert(res.ok);
    const actual = await kv.getBlob(["example"]);
    assert(actual.value);
    assertEquals(actual.versionstamp, res.versionstamp);
    assert(timingSafeEqual(actual.value, value));
    kv.close();
    return cleanup();
  },
});

Deno.test({
  name: "kvToolbox - encrypt/decrypt blob - Uint8Array - bypass encryption set",
  async fn() {
    const path = await getPath();
    const encryptWith = generateKey();
    const kv = await openKvToolbox({ path, encryptWith });
    const value = globalThis.crypto.getRandomValues(new Uint8Array(65_536));
    const res = await kv.setBlob(["example"], value, { encrypted: false });
    assert(res.ok);
    const actual = await kv.getBlob(["example"]);
    assertEquals(actual.value, null);
    kv.close();
    return cleanup();
  },
});

Deno.test({
  name:
    "kvToolbox - encrypt/decrypt blob - Uint8Array - bypass encryption get and set",
  async fn() {
    const path = await getPath();
    const encryptWith = generateKey();
    const kv = await openKvToolbox({ path, encryptWith });
    const value = globalThis.crypto.getRandomValues(new Uint8Array(65_536));
    const res = await kv.setBlob(["example"], value, { encrypted: false });
    assert(res.ok);
    const actual = await kv.getBlob(["example"], { encrypted: false });
    assert(actual.value);
    assertEquals(actual.versionstamp, res.versionstamp);
    assert(timingSafeEqual(actual.value, value));
    kv.close();
    return cleanup();
  },
});
