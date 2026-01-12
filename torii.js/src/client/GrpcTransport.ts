/**
 * GrpcTransport - Shared gRPC-Web transport layer
 * Handles encoding/decoding and HTTP communication
 */

import {
  encodeProtobufObject,
  decodeProtobufObject,
  encodeWithSchema,
  decodeWithSchema,
  frameMessage,
  type MessageSchema,
} from './protobuf';

export interface CallOptions {
  abort?: AbortSignal;
  timeout?: number;
  headers?: Record<string, string>;
  requestSchema?: MessageSchema;
  responseSchema?: MessageSchema;
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
    const messageBytes = options?.requestSchema
      ? encodeWithSchema(request, options.requestSchema)
      : encodeProtobufObject(request);
    const body = frameMessage(messageBytes);

    const response = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/grpc-web+proto',
        'Accept': 'application/grpc-web+proto',
        'x-grpc-web': '1',
        ...options?.headers,
      },
      body: body as BodyInit,
      signal: options?.abort,
    });

    if (!response.ok) {
      throw new Error(`gRPC call failed: ${response.status} ${response.statusText}`);
    }

    const responseBody = await response.arrayBuffer();
    return this.decodeMessageWithSchema<T>(new Uint8Array(responseBody), options?.responseSchema);
  }

  /**
   * Make a server-streaming gRPC call
   */
  async *streamCall<T = Record<string, unknown>>(
    path: string,
    request: Record<string, unknown>,
    options?: CallOptions
  ): AsyncGenerator<T> {
    const messageBytes = options?.requestSchema
      ? encodeWithSchema(request, options.requestSchema)
      : encodeProtobufObject(request);
    const body = frameMessage(messageBytes);

    const response = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/grpc-web+proto',
        'Accept': 'application/grpc-web+proto',
        'x-grpc-web': '1',
        ...options?.headers,
      },
      body: body as BodyInit,
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
          yield this.decodeMessageWithSchema<T>(messageFrame, options?.responseSchema);
        }
      }
    }
  }

  /**
   * Decode a gRPC-Web frame with optional schema
   */
  private decodeMessageWithSchema<T>(data: Uint8Array, schema?: MessageSchema): T {
    if (data.length < 5) return {} as T;

    const messageLength =
      (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];
    const message = data.slice(5, 5 + messageLength);

    if (message.length === 0) return {} as T;

    try {
      if (schema) {
        return decodeWithSchema<T>(message, schema);
      }
      return decodeProtobufObject(message) as T;
    } catch {
      return { _raw: Array.from(message) } as T;
    }
  }
}
