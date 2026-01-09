/**
 * BaseSinkClient - Abstract base class for all generated sink clients
 * Generated clients extend this class and use the protected methods
 */

import { GrpcTransport, type CallOptions } from './GrpcTransport';

export { CallOptions };

export abstract class BaseSinkClient {
  protected baseUrl: string;
  protected transport: GrpcTransport;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
    this.transport = new GrpcTransport(baseUrl);
  }

  /**
   * Make a unary RPC call
   * @param path - Full RPC path like '/package.Service/Method'
   * @param request - Request payload
   * @param options - Call options (abort, timeout, headers)
   */
  protected async unaryCall<T = Record<string, unknown>>(
    path: string,
    request: Record<string, unknown>,
    options?: CallOptions
  ): Promise<T> {
    return this.transport.unaryCall<T>(path, request, options);
  }

  /**
   * Make a server-streaming RPC call
   * @param path - Full RPC path like '/package.Service/Method'
   * @param request - Request payload
   * @param options - Call options (abort, timeout, headers)
   */
  protected async *streamCall<T = Record<string, unknown>>(
    path: string,
    request: Record<string, unknown>,
    options?: CallOptions
  ): AsyncGenerator<T> {
    yield* this.transport.streamCall<T>(path, request, options);
  }

  /**
   * Helper for subscription-style streaming with callbacks
   */
  protected async subscribeWithCallbacks<T>(
    path: string,
    request: Record<string, unknown>,
    onMessage: (message: T) => void,
    onError?: (error: Error) => void,
    onConnected?: () => void
  ): Promise<() => void> {
    const abortController = new AbortController();

    (async () => {
      try {
        onConnected?.();
        for await (const message of this.streamCall<T>(path, request, {
          abort: abortController.signal,
        })) {
          onMessage(message);
        }
      } catch (err: unknown) {
        if (err instanceof Error && err.name !== 'AbortError') {
          onError?.(err);
        }
      }
    })();

    return () => {
      abortController.abort();
    };
  }
}
