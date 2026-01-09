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

// CLI utilities (for programmatic usage)
export { generateFromReflection } from './cli/reflection';
export { generateFromProtos } from './cli/protos';
export {
  generateClientCode,
  type ServiceDefinition,
  type MethodDefinition,
  type GeneratorOptions,
} from './cli/generator';
