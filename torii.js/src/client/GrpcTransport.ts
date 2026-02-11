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
  onConnected?: () => void;
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

    const responseBody = new Uint8Array(await response.arrayBuffer());
    return this.decodeFramedResponse<T>(responseBody, options?.responseSchema);
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

    options?.onConnected?.();

    let buffer = new Uint8Array(0);

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const newBuffer = new Uint8Array(buffer.length + value.length);
      newBuffer.set(buffer);
      newBuffer.set(value, buffer.length);
      buffer = newBuffer;

      while (buffer.length >= 5) {
        const frameLength =
          (buffer[1] << 24) | (buffer[2] << 16) | (buffer[3] << 8) | buffer[4];
        const totalLength = 5 + frameLength;

        if (buffer.length < totalLength) break;

        const flag = buffer[0];
        const payload = buffer.slice(5, totalLength);
        buffer = buffer.slice(totalLength);

        if (flag === 0x00) {
          yield this.decodePayload<T>(payload, options?.responseSchema);
        } else if (flag === 0x80) {
          GrpcTransport.checkTrailer(payload);
        }
      }
    }
  }

  /**
   * Parse a gRPC-Web response buffer that may contain data + trailer frames.
   *
   * Frame format: 1-byte flag | 4-byte big-endian length | payload
   *   flag 0x00 = data frame (protobuf message)
   *   flag 0x80 = trailer frame (ASCII "key: value\r\n" pairs)
   */
  private decodeFramedResponse<T>(data: Uint8Array, schema?: MessageSchema): T {
    if (data.length < 5) return {} as T;

    let offset = 0;
    let messageData: Uint8Array | null = null;

    while (offset + 5 <= data.length) {
      const flag = data[offset];
      const frameLength =
        (data[offset + 1] << 24) |
        (data[offset + 2] << 16) |
        (data[offset + 3] << 8) |
        data[offset + 4];
      const frameEnd = offset + 5 + frameLength;
      if (frameEnd > data.length) break;

      if (flag === 0x00) {
        messageData = data.slice(offset + 5, frameEnd);
      } else if (flag === 0x80) {
        GrpcTransport.checkTrailer(data.slice(offset + 5, frameEnd));
      }

      offset = frameEnd;
    }

    if (!messageData || messageData.length === 0) return {} as T;

    return this.decodePayload<T>(messageData, schema);
  }

  private decodePayload<T>(message: Uint8Array, schema?: MessageSchema): T {
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

  private static checkTrailer(payload: Uint8Array): void {
    const trailer = new TextDecoder().decode(payload);
    const statusMatch = trailer.match(/grpc-status:\s*(\d+)/);
    if (statusMatch) {
      const grpcStatus = parseInt(statusMatch[1], 10);
      if (grpcStatus !== 0) {
        const msgMatch = trailer.match(/grpc-message:\s*([^\r\n]*)/);
        const grpcMessage = msgMatch
          ? decodeURIComponent(msgMatch[1])
          : `gRPC error ${grpcStatus}`;
        throw new Error(grpcMessage);
      }
    }
  }
}
