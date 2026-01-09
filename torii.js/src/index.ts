// torii.js - TypeScript client library for Torii gRPC services
// Re-export types and utilities for library consumers

export { generateFromReflection } from './cli/reflection';
export { generateFromProtos } from './cli/protos';
export { generateClientCode, type ServiceDefinition, type MethodDefinition } from './cli/generator';

export {
  BaseToriiClient,
  type CallOptions,
  type TopicSubscription,
  type TopicUpdate,
} from './client/ToriiClient';
