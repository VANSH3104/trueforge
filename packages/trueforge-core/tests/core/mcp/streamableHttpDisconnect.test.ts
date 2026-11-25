import http from 'node:http';
import type { Logger } from 'winston';
import { RemoteMCP } from '../../../src/core/mcp/RemoteMCP';
import { connectRemoteMcp } from '../../../src/core/mcp/remoteMcpClient';
import { NOOP_AGENT_TRACING } from '../../../src/core/tracing/NoopAgentTracing';
import { configureOutboundUrlGuard } from '../../../src/core/util/ssrfGuard';

describe('StreamableHTTPClientTransport reconnection crash on server disconnect', () => {
  let server: http.Server;
  let sseResponse: http.ServerResponse | null = null;
  let port: number;

  beforeEach(done => {
    sseResponse = null;
    configureOutboundUrlGuard({ allowedHosts: ['127.0.0.1', 'localhost'], blockedHosts: [] });
    server = http.createServer((req, res) => {
      if (req.method === 'GET') {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          Connection: 'keep-alive',
        });
        res.write('id: 1\nevent: endpoint\ndata: /mcp\n\n');
        sseResponse = res;
        return;
      }
      if (req.method === 'POST') {
        let body = '';
        req.on('data', chunk => {
          body += chunk;
        });
        req.on('end', () => {
          const parsed = JSON.parse(body);
          if (parsed.method === 'initialize') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(
              JSON.stringify({
                jsonrpc: '2.0',
                id: parsed.id,
                result: {
                  protocolVersion: '2024-11-05',
                  capabilities: { tools: {} },
                  serverInfo: { name: 'test-server', version: '1.0.0' },
                },
              }),
            );
            return;
          }
          if (parsed.method === 'notifications/initialized') {
            res.writeHead(202);
            res.end();
            return;
          }
          if (parsed.method === 'tools/list') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(
              JSON.stringify({
                jsonrpc: '2.0',
                id: parsed.id,
                result: {
                  tools: [{ name: 'ping', inputSchema: { type: 'object' } }],
                },
              }),
            );
            return;
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: parsed.id, result: {} }));
        });
        return;
      }
      res.writeHead(404).end();
    });

    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      if (typeof addr === 'object' && addr !== null) {
        port = addr.port;
      }
      done();
    });
  });

  afterEach(done => {
    if (server.listening) {
      server.close(() => done());
    } else {
      done();
    }
  });

  it('recovers cleanly without unhandled rejection when server drops', async () => {
    const unhandledRejections: unknown[] = [];
    const onUnhandled = (reason: unknown) => {
      unhandledRejections.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);

    try {
      const clientErrors: unknown[] = [];
      let closed = false;
      const conn = await connectRemoteMcp({
        url: `http://127.0.0.1:${port}/mcp`,
        headers: {},
        knownTransportType: 'streamable-http',
        requestTimeoutMs: 5000,
        connectTimeoutMs: 5000,
        signal: new AbortController().signal,
        onClose: () => {
          closed = true;
        },
        onError: err => {
          clientErrors.push(err);
        },
      });

      // Wait for GET SSE stream to connect
      for (let i = 0; i < 50 && !sseResponse; i++) {
        await new Promise(r => setTimeout(r, 20));
      }
      expect(sseResponse).not.toBeNull();

      // Now shut down the server and destroy the SSE stream
      sseResponse?.destroy();
      await new Promise<void>(resolve => server.close(() => resolve()));

      // Closing connection directly or letting reconnection fail is safe and clean
      await conn.close();

      expect(unhandledRejections).toHaveLength(0);
      expect(closed).toBe(true);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('triggers onClose when reconnection attempts are exhausted after server drops', async () => {
    const unhandledRejections: unknown[] = [];
    const onUnhandled = (reason: unknown) => {
      unhandledRejections.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);

    try {
      const clientErrors: unknown[] = [];
      let closed = false;
      const conn = await connectRemoteMcp({
        url: `http://127.0.0.1:${port}/mcp`,
        headers: {},
        knownTransportType: 'streamable-http',
        requestTimeoutMs: 5000,
        connectTimeoutMs: 5000,
        signal: new AbortController().signal,
        onClose: () => {
          closed = true;
        },
        onError: err => {
          clientErrors.push(err);
        },
      });

      for (let i = 0; i < 50 && !sseResponse; i++) {
        await new Promise(r => setTimeout(r, 20));
      }
      expect(sseResponse).not.toBeNull();

      sseResponse?.destroy();
      await new Promise<void>(resolve => server.close(() => resolve()));

      // Wait for reconnection retries to exceed maxRetries and trigger onClose
      for (let i = 0; i < 200 && !closed; i++) {
        await new Promise(r => setTimeout(r, 50));
      }

      await conn.close();
      expect(unhandledRejections).toHaveLength(0);
      expect(closed).toBe(true);
      expect(clientErrors.some(e => e instanceof Error && e.message.includes('Maximum reconnection attempts'))).toBe(
        true,
      );
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('resets RemoteMCP connection state when reconnect attempts are exhausted and reconnects on next call', async () => {
    const logger: Pick<Logger, 'info' | 'warn' | 'error' | 'debug'> = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    };
    const remote = new RemoteMCP({
      id: 'test-mcp',
      connection: {
        name: 'test-mcp',
        description: 'Test MCP',
        url: `http://127.0.0.1:${port}/mcp`,
      },
      tracing: NOOP_AGENT_TRACING,
      logger: logger as Logger,
      requestTimeoutMs: 5000,
      connectTimeoutMs: 5000,
      signal: new AbortController().signal,
    });

    try {
      const firstTools = await remote.listTools();
      expect('result' in firstTools && firstTools.result.tools.map(t => t.name)).toEqual(['ping']);

      for (let i = 0; i < 50 && !sseResponse; i++) {
        await new Promise(r => setTimeout(r, 20));
      }
      expect(sseResponse).not.toBeNull();

      // Drop server and wait for reconnection attempts to exhaust
      sseResponse?.destroy();
      await new Promise<void>(resolve => server.close(() => resolve()));

      const errorMock = logger.error as jest.Mock;
      for (let i = 0; i < 200; i++) {
        const loggedExhausted = errorMock.mock.calls.some(args =>
          JSON.stringify(args).includes('Maximum reconnection attempts'),
        );
        if (loggedExhausted) {
          break;
        }
        await new Promise(r => setTimeout(r, 50));
      }

      // Restart server on the same port and verify RemoteMCP reconnects instead of staying stuck
      sseResponse = null;
      await new Promise<void>(resolve => server.listen(port, '127.0.0.1', () => resolve()));

      const secondTools = await remote.listTools();
      expect('result' in secondTools && secondTools.result.tools.map(t => t.name)).toEqual(['ping']);
    } finally {
      await remote.close();
    }
  });
});
