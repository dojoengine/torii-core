/**
 * ToriiClient - Generic plugin-based client for Torii gRPC services
 *
 * Usage:
 * ```typescript
 * import { ToriiClient } from "@toriijs/sdk";
 * import { SqlSinkClient, LogSinkClient } from "./generated";
 *
 * const client = new ToriiClient("http://localhost:8080", {
 *   sql: SqlSinkClient,
 *   log: LogSinkClient,
 * });
 *
 * // Fully typed access
 * const result = await client.sql.query("SELECT * FROM users");
 * ```
 */

import { BaseSinkClient, type CallOptions } from './BaseSinkClient';
import { GrpcTransport } from './GrpcTransport';

// Type for a client class constructor
type ClientClass<T extends BaseSinkClient = BaseSinkClient> = new (baseUrl: string) => T;

// Map of plugin names to client classes
type ClientMap = Record<string, ClientClass>;

// Transform client classes to instances
type InstantiatedClients<T extends ClientMap> = {
  [K in keyof T]: InstanceType<T[K]>;
};

export interface TopicSubscription {
  topic: string;
  filters?: Record<string, string>;
}

export interface TopicUpdate {
  topic: string;
  updateType: number;
  timestamp: number;
  typeId: string;
  data?: unknown;
}

export interface ToriiClientOptions<T extends ClientMap = ClientMap> {
  plugins?: T;
}

/**
 * Main Torii client with plugin support
 */
class ToriiClientImpl<T extends ClientMap = {}> {
  private _baseUrl: string;
  private _transport: GrpcTransport;
  private _clients: InstantiatedClients<T>;
  private _abortController: AbortController | null = null;

  constructor(baseUrl: string, plugins?: T) {
    this._baseUrl = baseUrl;
    this._transport = new GrpcTransport(baseUrl);

    // Instantiate all plugin clients
    this._clients = {} as InstantiatedClients<T>;
    if (plugins) {
      for (const [key, ClientClass] of Object.entries(plugins)) {
        (this._clients as Record<string, BaseSinkClient>)[key] = new ClientClass(baseUrl);
      }
    }
  }

  get baseUrl(): string {
    return this._baseUrl;
  }

  // ==================
  // Core Torii Methods
  // ==================

  /**
   * Get server version
   */
  async getVersion(): Promise<{ version: string; buildTime: string }> {
    const response = await this._transport.unaryCall<{
      version?: string;
      build_time?: string;
    }>('/torii.Torii/GetVersion', {});
    return {
      version: response.version ?? '',
      buildTime: response.build_time ?? '',
    };
  }

  /**
   * List available topics from all registered sinks
   */
  async listTopics(): Promise<
    Array<{
      name: string;
      sinkName: string;
      availableFilters: string[];
      description: string;
    }>
  > {
    const response = await this._transport.unaryCall<{
      topics?: Array<{
        name?: string;
        sink_name?: string;
        available_filters?: string[];
        description?: string;
      }>;
    }>('/torii.Torii/ListTopics', {});

    return (response.topics ?? []).map((t) => ({
      name: t.name ?? '',
      sinkName: t.sink_name ?? '',
      availableFilters: t.available_filters ?? [],
      description: t.description ?? '',
    }));
  }

  /**
   * Subscribe to topics with server-side streaming
   */
  async subscribeTopics(
    clientId: string,
    topics: TopicSubscription[],
    onUpdate: (update: TopicUpdate) => void,
    onError?: (error: Error) => void,
    onConnected?: () => void
  ): Promise<() => void> {
    this._abortController = new AbortController();

    const request = {
      client_id: clientId,
      topics: topics.map((t) => ({
        topic: t.topic,
        filters: t.filters ?? {},
      })),
      unsubscribe_topics: [],
    };

    (async () => {
      try {
        onConnected?.();
        for await (const response of this._transport.streamCall<{
          topic?: string;
          update_type?: number;
          timestamp?: string | number;
          type_id?: string;
          data?: unknown;
        }>('/torii.Torii/SubscribeToTopicsStream', request, {
          abort: this._abortController?.signal,
        })) {
          onUpdate({
            topic: response.topic ?? '',
            updateType: response.update_type ?? 0,
            timestamp: Number(response.timestamp ?? 0),
            typeId: response.type_id ?? '',
            data: response.data,
          });
        }
      } catch (err: unknown) {
        if (err instanceof Error && err.name !== 'AbortError') {
          onError?.(err);
        }
      }
    })();

    return () => {
      this._abortController?.abort();
      this._abortController = null;
    };
  }

  /**
   * Disconnect any active subscriptions
   */
  disconnect(): void {
    this._abortController?.abort();
    this._abortController = null;
  }
}

// Create the ToriiClient with Proxy for plugin access
export function createToriiClient<T extends ClientMap>(
  baseUrl: string,
  plugins?: T
): ToriiClientImpl<T> & InstantiatedClients<T> {
  const client = new ToriiClientImpl(baseUrl, plugins);

  return new Proxy(client, {
    get(target, prop, receiver) {
      // First check if it's a property of the client itself
      if (prop in target) {
        const value = Reflect.get(target, prop, receiver);
        if (typeof value === 'function') {
          return value.bind(target);
        }
        return value;
      }
      // Then check if it's a plugin
      if (prop in (target as any)._clients) {
        return (target as any)._clients[prop];
      }
      return undefined;
    },
  }) as ToriiClientImpl<T> & InstantiatedClients<T>;
}

// Export a class-like interface for familiar usage pattern
export const ToriiClient = createToriiClient as unknown as {
  new <T extends ClientMap = {}>(
    baseUrl: string,
    plugins?: T
  ): ToriiClientImpl<T> & InstantiatedClients<T>;
};

// Type declaration to make ToriiClient work as a class
export type ToriiClient<T extends ClientMap = {}> = ToriiClientImpl<T> &
  InstantiatedClients<T>;
