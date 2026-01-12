import { generateClientCode, type ServiceDefinition, type GeneratorOptions } from './generator';
import { fromBinary } from '@bufbuild/protobuf';
import { FileDescriptorProtoSchema, type FileDescriptorProto } from '@bufbuild/protobuf/wkt';
import {
  extractTypesFromFileDescriptor,
  generateTypesFile,
  generateSchemaFile,
  type MessageDefinition,
  type EnumDefinition,
} from './types-generator';

export async function generateFromReflection(
  serverUrl: string,
  outputDir: string,
  options: Partial<GeneratorOptions> = {}
): Promise<void> {
  console.log('Connecting to server for reflection...');

  const services = await listServices(serverUrl);
  console.log(`Found ${services.length} services:`);
  services.forEach((s) => console.log(`  - ${s}`));

  const filteredServices = services.filter(
    (s) =>
      !s.startsWith('grpc.reflection') && !s.startsWith('grpc.health')
  );

  if (filteredServices.length === 0) {
    throw new Error('No application services found via reflection');
  }

  // Collect all FileDescriptorProtos
  const allFileDescriptors = new Map<string, FileDescriptorProto>();
  const allMessages = new Map<string, MessageDefinition>();
  const allEnums = new Map<string, EnumDefinition>();

  console.log(`\nCollecting type definitions...`);

  const serviceDefinitions: ServiceDefinition[] = [];
  for (const serviceName of filteredServices) {
    const { definition, fileDescriptors } = await getServiceDefinitionWithTypes(serverUrl, serviceName);
    if (definition) {
      serviceDefinitions.push(definition);
    }
    // Collect file descriptors (deduplicated by name)
    for (const fd of fileDescriptors) {
      if (fd.name && !allFileDescriptors.has(fd.name)) {
        allFileDescriptors.set(fd.name, fd);
        extractTypesFromFileDescriptor(fd, allMessages, allEnums);
      }
    }
  }

  console.log(`Found ${allMessages.size} message types, ${allEnums.size} enums`);

  // Generate types and schema files
  console.log(`\nGenerating type definitions...`);
  await generateTypesFile(allMessages, allEnums, outputDir);
  await generateSchemaFile(allMessages, allEnums, outputDir);

  // Generate client code with type information
  console.log(`\nGenerating clients for ${filteredServices.length} services...`);
  await generateClientCode(serviceDefinitions, outputDir, options, allMessages);
}

async function listServices(serverUrl: string): Promise<string[]> {
  const request = createListServicesRequest();
  const response = await callReflection(serverUrl, request);

  const services: string[] = [];
  parseListServicesResponse(response, services);

  return services;
}

interface ServiceDefinitionResult {
  definition: ServiceDefinition | null;
  fileDescriptors: FileDescriptorProto[];
}

async function getServiceDefinitionWithTypes(
  serverUrl: string,
  serviceName: string
): Promise<ServiceDefinitionResult> {
  try {
    const request = createFileContainingSymbolRequest(serviceName);
    const response = await callReflection(serverUrl, request);

    const fileDescriptors = parseFileDescriptorResponse(response);

    for (const fd of fileDescriptors) {
      for (const service of fd.service) {
        const fullName = fd.package ? `${fd.package}.${service.name}` : service.name;
        if (fullName === serviceName) {
          return {
            definition: {
              name: service.name,
              fullName,
              package: fd.package,
              methods: service.method.map((m) => ({
                name: m.name,
                inputType: m.inputType.replace(/^\./, ''),
                outputType: m.outputType.replace(/^\./, ''),
                inputTypeShort: m.inputType.split('.').pop() || m.inputType,
                outputTypeShort: m.outputType.split('.').pop() || m.outputType,
                clientStreaming: m.clientStreaming ?? false,
                serverStreaming: m.serverStreaming ?? false,
              })),
            },
            fileDescriptors,
          };
        }
      }
    }

    // Fallback: return service without methods if not found in descriptors
    const parts = serviceName.split('.');
    return {
      definition: {
        name: parts[parts.length - 1],
        fullName: serviceName,
        package: parts.slice(0, -1).join('.'),
        methods: [],
      },
      fileDescriptors,
    };
  } catch (error) {
    console.warn(`Warning: Failed to get service definition for ${serviceName}`);
    return { definition: null, fileDescriptors: [] };
  }
}

async function callReflection(serverUrl: string, request: Uint8Array): Promise<Uint8Array> {
  const url = `${serverUrl}/grpc.reflection.v1.ServerReflection/ServerReflectionInfo`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/grpc-web+proto',
      'Accept': 'application/grpc-web+proto',
      'x-grpc-web': '1',
    },
    body: request,
  });

  if (!response.ok) {
    throw new Error(`Reflection request failed: ${response.status} ${response.statusText}`);
  }

  const responseBody = await response.arrayBuffer();
  return new Uint8Array(responseBody);
}

function createListServicesRequest(): Uint8Array {
  // ServerReflectionRequest with list_services = "" (field 7)
  const message = new Uint8Array([0x3a, 0x00]); // field 7, length 0
  return frameMessage(message);
}

function createFileContainingSymbolRequest(symbol: string): Uint8Array {
  // ServerReflectionRequest with file_containing_symbol (field 4)
  const symbolBytes = new TextEncoder().encode(symbol);
  const message = new Uint8Array(2 + symbolBytes.length);
  message[0] = 0x22; // field 4, wire type 2 (length-delimited)
  message[1] = symbolBytes.length;
  message.set(symbolBytes, 2);
  return frameMessage(message);
}

function frameMessage(message: Uint8Array): Uint8Array {
  const frame = new Uint8Array(5 + message.length);
  frame[0] = 0x00; // not compressed
  const len = message.length;
  frame[1] = (len >> 24) & 0xff;
  frame[2] = (len >> 16) & 0xff;
  frame[3] = (len >> 8) & 0xff;
  frame[4] = len & 0xff;
  frame.set(message, 5);
  return frame;
}

function parseListServicesResponse(data: Uint8Array, services: string[]): void {
  // Skip gRPC frame header (5 bytes)
  if (data.length < 5) return;

  const messageLength = (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];
  const message = data.slice(5, 5 + messageLength);

  let offset = 0;
  while (offset < message.length) {
    const tag = message[offset++];
    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) { // length-delimited
      const length = readVarint(message, offset);
      offset += varintSize(length);
      const fieldData = message.slice(offset, offset + length);
      offset += length;

      // Field 6 is list_services_response
      if (fieldNumber === 6) {
        parseServiceResponseList(fieldData, services);
      }
    } else if (wireType === 0) { // varint
      readVarint(message, offset);
      offset += varintSize(readVarint(message, offset));
    }
  }
}

function parseServiceResponseList(data: Uint8Array, services: string[]): void {
  let offset = 0;
  while (offset < data.length) {
    const tag = data[offset++];
    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) {
      const length = readVarint(data, offset);
      offset += varintSize(length);
      const fieldData = data.slice(offset, offset + length);
      offset += length;

      // Field 1 is repeated ServiceResponse
      if (fieldNumber === 1) {
        const name = parseServiceResponse(fieldData);
        if (name) services.push(name);
      }
    }
  }
}

function parseServiceResponse(data: Uint8Array): string | null {
  let offset = 0;
  while (offset < data.length) {
    const tag = data[offset++];
    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) {
      const length = readVarint(data, offset);
      offset += varintSize(length);
      const fieldData = data.slice(offset, offset + length);
      offset += length;

      // Field 1 is name
      if (fieldNumber === 1) {
        return new TextDecoder().decode(fieldData);
      }
    }
  }
  return null;
}

function parseFileDescriptorResponse(data: Uint8Array): FileDescriptorProto[] {
  const descriptors: FileDescriptorProto[] = [];

  // Skip gRPC frame header
  if (data.length < 5) return descriptors;

  const messageLength = (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];
  const message = data.slice(5, 5 + messageLength);

  let offset = 0;
  while (offset < message.length) {
    const tag = message[offset++];
    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) {
      const length = readVarint(message, offset);
      offset += varintSize(length);
      const fieldData = message.slice(offset, offset + length);
      offset += length;

      // Field 4 is file_descriptor_response
      if (fieldNumber === 4) {
        parseFileDescriptorResponseInner(fieldData, descriptors);
      }
    } else if (wireType === 0) {
      const val = readVarint(message, offset);
      offset += varintSize(val);
    }
  }

  return descriptors;
}

function parseFileDescriptorResponseInner(data: Uint8Array, descriptors: FileDescriptorProto[]): void {
  let offset = 0;
  while (offset < data.length) {
    const tag = data[offset++];
    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) {
      const length = readVarint(data, offset);
      offset += varintSize(length);
      const fieldData = data.slice(offset, offset + length);
      offset += length;

      // Field 1 is repeated file_descriptor_proto (bytes)
      if (fieldNumber === 1) {
        try {
          const fd = fromBinary(FileDescriptorProtoSchema, fieldData);
          descriptors.push(fd);
        } catch (e) {
          console.warn('Failed to parse FileDescriptorProto');
        }
      }
    }
  }
}

function readVarint(data: Uint8Array, offset: number): number {
  let result = 0;
  let shift = 0;
  let byte: number;
  do {
    byte = data[offset++];
    result |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);
  return result;
}

function varintSize(value: number): number {
  let size = 1;
  while (value >= 0x80) {
    value >>= 7;
    size++;
  }
  return size;
}
