"use strict";

import assert from "node:assert/strict";

import { bindLogger, setLevel } from "@replit/river/logging";
import { createClient } from "@replit/river";
import { BinaryCodec } from '@replit/river/codec';
import { WebSocketClientTransport } from "@replit/river/transport/ws/client";
import WebSocket from "isomorphic-ws";

import type { TestServiceConstructor } from "./server/test";

async function main() {
  bindLogger((msg) => console.error(msg), true);
  setLevel("info");
  try {
    const transport = new WebSocketClientTransport(
      async () => new WebSocket(new URL("http://localhost:50052")),
      "client",
      "SERVER",
      {
        codec: BinaryCodec,
      },
    );

    const client = createClient<TestServiceConstructor>(transport);

    // Test RPC.
    const result = await client.test.rpc({
      request: {
        $kind: "string",
        string: "hello",
      },
    });
    assert.deepEqual({ ok: true, payload: { payload: "hello" } }, result);

    // Test stream
    // TODO: River gets confused about streams, so we can't send multiple messages for now.
    const [input, output, close] = await client.test.stream();
    await input.push({ payload: "hello" });
    await input.end();
    const response = (await output.next()).value;
    assert.equal(response.ok, true);
    assert.deepEqual(
      { ok: true, payload: { payload: "hello" } },
      response,
    );
    close();
    client.close();
    transport.close();
  } catch (error) {
    console.error("test failed", { error });
    throw error;
  }
}

const timeout = setTimeout(() => {
  console.error("test timed out");
  process.exit(1);
}, 10000);
await main();
clearTimeout(timeout);
timeout.unref();
process.exit(0);
