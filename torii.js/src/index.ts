// @toriijs/sdk - TypeScript client library for Torii gRPC services

// Core client
export {
  ToriiClient,
  createToriiClient,
  type TopicSubscription,
  type TopicUpdate,
} from './client/ToriiClient';

// Base class for generated clients
export { BaseSinkClient, type CallOptions } from './client/BaseSinkClient';

// Transport layer (for advanced usage)
export { GrpcTransport } from './client/GrpcTransport';

// Schema types for protobuf encoding/decoding
export {
  setSchemaRegistry,
  getSchemaRegistry,
  encodeWithSchema,
  decodeWithSchema,
  type MessageSchema,
  type FieldSchema,
} from './client/protobuf';
