/**
 * GrpcTransport - Shared gRPC-Web transport layer
 * Handles encoding/decoding and HTTP communication
 */

export interface CallOptions {
  abort?: AbortSignal;
  timeout?: number;
  headers?: Record<string, string>;
}

export class GrpcTransport {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
  }

  /**
   * Make a unary gRPC call
   */
  async unaryCall<T = Record<string, unknown>>(
    path: string,
    request: Record<string, unknown>,
    options?: CallOptions
  ): Promise<T> {
    const body = this.encodeMessage(request);

    const response = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/grpc-web+proto',
        'Accept': 'application/grpc-web+proto',
        'x-grpc-web': '1',
        ...options?.headers,
      },
      body,
      signal: options?.abort,
    });

    if (!response.ok) {
      throw new Error(`gRPC call failed: ${response.status} ${response.statusText}`);
    }

    const responseBody = await response.arrayBuffer();
    return this.decodeMessage(new Uint8Array(responseBody)) as T;
  }

  /**
   * Make a server-streaming gRPC call
   */
  async *streamCall<T = Record<string, unknown>>(
    path: string,
    request: Record<string, unknown>,
    options?: CallOptions
  ): AsyncGenerator<T> {
    const body = this.encodeMessage(request);

    const response = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/grpc-web+proto',
        'Accept': 'application/grpc-web+proto',
        'x-grpc-web': '1',
        ...options?.headers,
      },
      body,
      signal: options?.abort,
    });

    if (!response.ok) {
      throw new Error(`gRPC call failed: ${response.status} ${response.statusText}`);
    }

    const reader = response.body?.getReader();
    if (!reader) throw new Error('No response body');

    let buffer = new Uint8Array(0);

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      // Append to buffer
      const newBuffer = new Uint8Array(buffer.length + value.length);
      newBuffer.set(buffer);
      newBuffer.set(value, buffer.length);
      buffer = newBuffer;

      // Parse complete frames from buffer
      while (buffer.length >= 5) {
        const messageLength =
          (buffer[1] << 24) | (buffer[2] << 16) | (buffer[3] << 8) | buffer[4];
        const totalLength = 5 + messageLength;

        if (buffer.length < totalLength) break;

        const messageFrame = buffer.slice(0, totalLength);
        buffer = buffer.slice(totalLength);

        // Only yield data frames (0x00), skip trailers (0x80)
        if (messageFrame[0] === 0x00) {
          yield this.decodeMessage(messageFrame) as T;
        }
      }
    }
  }

  /**
   * Encode a message for gRPC-Web transport
   */
  private encodeMessage(message: Record<string, unknown>): Uint8Array {
    const json = JSON.stringify(message);
    const messageBytes = new TextEncoder().encode(json);

    // gRPC-Web frame: 1 byte flag + 4 bytes length + message
    const frame = new Uint8Array(5 + messageBytes.length);
    frame[0] = 0x00; // Data frame
    const len = messageBytes.length;
    frame[1] = (len >> 24) & 0xff;
    frame[2] = (len >> 16) & 0xff;
    frame[3] = (len >> 8) & 0xff;
    frame[4] = len & 0xff;
    frame.set(messageBytes, 5);

    return frame;
  }

  /**
   * Decode a gRPC-Web frame
   */
  private decodeMessage(data: Uint8Array): Record<string, unknown> {
    if (data.length < 5) return {};

    const messageLength =
      (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];
    const message = data.slice(5, 5 + messageLength);

    try {
      const text = new TextDecoder().decode(message);
      return JSON.parse(text);
    } catch {
      // Return raw bytes if not JSON
      return { _raw: Array.from(message) };
    }
  }
}
