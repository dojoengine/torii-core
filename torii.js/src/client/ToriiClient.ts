// Base ToriiClient class that can be extended by generated clients
// This provides a foundation for the generated aggregated client

export interface CallOptions {
  abort?: AbortSignal;
  timeout?: number;
  headers?: Record<string, string>;
}

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

export class BaseToriiClient {
  protected baseUrl: string;
  private abortController: AbortController | null = null;

  constructor(baseUrl: string = 'http://localhost:8080') {
    this.baseUrl = baseUrl.replace(/\/$/, '');
  }

  async getVersion(): Promise<{ version: string; buildTime: string }> {
    const response = await this.makeUnaryCall('/torii.Torii/GetVersion', {});
    return {
      version: (response as any).version ?? '',
      buildTime: (response as any).build_time ?? '',
    };
  }

  async listTopics(): Promise<
    Array<{
      name: string;
      sinkName: string;
      availableFilters: string[];
      description: string;
    }>
  > {
    const response = await this.makeUnaryCall('/torii.Torii/ListTopics', {});
    const topics = (response as any).topics ?? [];
    return topics.map((t: any) => ({
      name: t.name,
      sinkName: t.sink_name,
      availableFilters: t.available_filters ?? [],
      description: t.description ?? '',
    }));
  }

  async subscribe(
    clientId: string,
    topics: TopicSubscription[],
    onUpdate: (update: TopicUpdate) => void,
    onError?: (error: Error) => void,
    onConnected?: () => void
  ): Promise<() => void> {
    this.abortController = new AbortController();

    const request = {
      client_id: clientId,
      topics: topics.map((t) => ({
        topic: t.topic,
        filters: t.filters ?? {},
      })),
      unsubscribe_topics: [],
    };

    const body = this.encodeMessage(request);

    try {
      const response = await fetch(
        `${this.baseUrl}/torii.Torii/SubscribeToTopicsStream`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/grpc-web+proto',
            Accept: 'application/grpc-web+proto',
            'x-grpc-web': '1',
          },
          body,
          signal: this.abortController.signal,
        }
      );

      if (!response.ok) {
        throw new Error(`Subscribe failed: ${response.status}`);
      }

      onConnected?.();

      const reader = response.body?.getReader();
      if (!reader) throw new Error('No response body');

      (async () => {
        let buffer = new Uint8Array(0);

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            const newBuffer = new Uint8Array(buffer.length + value.length);
            newBuffer.set(buffer);
            newBuffer.set(value, buffer.length);
            buffer = newBuffer;

            while (buffer.length >= 5) {
              const messageLength =
                (buffer[1] << 24) |
                (buffer[2] << 16) |
                (buffer[3] << 8) |
                buffer[4];
              const totalLength = 5 + messageLength;

              if (buffer.length < totalLength) break;

              const messageFrame = buffer.slice(0, totalLength);
              buffer = buffer.slice(totalLength);

              if (messageFrame[0] === 0x00) {
                const decoded = this.decodeMessage(messageFrame);
                onUpdate({
                  topic: (decoded as any).topic ?? '',
                  updateType: (decoded as any).update_type ?? 0,
                  timestamp: Number((decoded as any).timestamp ?? 0),
                  typeId: (decoded as any).type_id ?? '',
                  data: (decoded as any).data,
                });
              }
            }
          }
        } catch (err: any) {
          if (err.name !== 'AbortError') {
            onError?.(err);
          }
        }
      })();

      return () => {
        this.abortController?.abort();
        this.abortController = null;
      };
    } catch (error: any) {
      onError?.(error);
      return () => {};
    }
  }

  protected async makeUnaryCall(
    path: string,
    request: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const body = this.encodeMessage(request);

    const response = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/grpc-web+proto',
        Accept: 'application/grpc-web+proto',
        'x-grpc-web': '1',
      },
      body,
    });

    if (!response.ok) {
      throw new Error(`gRPC call failed: ${response.status} ${response.statusText}`);
    }

    const responseBody = await response.arrayBuffer();
    return this.decodeMessage(new Uint8Array(responseBody));
  }

  protected encodeMessage(message: Record<string, unknown>): Uint8Array {
    const json = JSON.stringify(message);
    const messageBytes = new TextEncoder().encode(json);

    const frame = new Uint8Array(5 + messageBytes.length);
    frame[0] = 0x00;
    const len = messageBytes.length;
    frame[1] = (len >> 24) & 0xff;
    frame[2] = (len >> 16) & 0xff;
    frame[3] = (len >> 8) & 0xff;
    frame[4] = len & 0xff;
    frame.set(messageBytes, 5);

    return frame;
  }

  protected decodeMessage(data: Uint8Array): Record<string, unknown> {
    if (data.length < 5) return {};

    const messageLength =
      (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];
    const message = data.slice(5, 5 + messageLength);

    try {
      const text = new TextDecoder().decode(message);
      return JSON.parse(text);
    } catch {
      return { _raw: Array.from(message) };
    }
  }

  disconnect(): void {
    this.abortController?.abort();
    this.abortController = null;
  }
}
