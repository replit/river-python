"use strict";

import http from 'node:http';

import { bindLogger, setLevel } from "@replit/river/logging";
import { createServer, serializeService } from "@replit/river";
import { WebSocketServerTransport } from "@replit/river/transport/ws/server";
import { BinaryCodec } from "@replit/river/codec";
import { WebSocketServer } from 'ws';

import { ServiceBuilder, Ok, buildServiceDefs } from '@replit/river';
import { Type } from '@sinclair/typebox';

export const TestServiceConstructor = () =>
  ServiceBuilder.create('test')
    .initialState({})
    .defineProcedure('echo', {
      type: 'rpc',
      input: Type.Object({ s: Type.String() }),
      output: Type.Object({ s: Type.String() }),
      errors: Type.Never(),
      async handler(_ctx, { s }) {
        return Ok({ s });
      },
    })
    .finalize();

export const serviceDefs = buildServiceDefs([TestServiceConstructor()]);

async function main() {
  if (process.argv.includes('--dump-schema')) {
    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(Object.values(serviceDefs).map(serializeService), null, 2),
    );
    process.exit(0);
  }
  
  bindLogger((msg) => console.error(msg), true);
  setLevel("debug");
  try {
    const httpServer = http.createServer();
    const port = parseInt(process.argv[2], 10) || 50052;
    const wss = new WebSocketServer({ server: httpServer });
    const transport = new WebSocketServerTransport(wss, 'SERVER', {
      codec: BinaryCodec,
    });

    // This has the side effect of binding the server to the transport
    // (and transitively, to the HTTP server).
    createServer(transport, serviceDefs);

    httpServer.listen(port, () => {
      const addr = httpServer.address();
      if (typeof addr === 'object' && addr) {
        console.log('SERVER UP');
      } else {
        throw new Error("couldn't find a port to allocate");
      }
    });
  } catch (error) {
    console.error("test failed", { error });
    throw error;
  }
}

await main();