import { generateClientCode, type ServiceDefinition } from './generator';

interface ReflectionResponse {
  listServicesResponse?: {
    service: Array<{ name: string }>;
  };
  fileDescriptorResponse?: {
    fileDescriptorProto: Uint8Array[];
  };
  errorResponse?: {
    errorCode: number;
    errorMessage: string;
  };
}

export async function generateFromReflection(
  serverUrl: string,
  outputDir: string
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

  console.log(`\nGenerating clients for ${filteredServices.length} services...`);

  const serviceDefinitions: ServiceDefinition[] = [];
  for (const serviceName of filteredServices) {
    const definition = await getServiceDefinition(serverUrl, serviceName);
    if (definition) {
      serviceDefinitions.push(definition);
    }
  }

  await generateClientCode(serviceDefinitions, outputDir);
}

async function listServices(serverUrl: string): Promise<string[]> {
  const response = await callReflection(serverUrl, {
    listServices: '',
  });

  if (response.errorResponse) {
    throw new Error(
      `Reflection error: ${response.errorResponse.errorMessage}`
    );
  }

  if (!response.listServicesResponse?.service) {
    throw new Error('No services returned from reflection');
  }

  return response.listServicesResponse.service.map((s) => s.name);
}

async function getServiceDefinition(
  serverUrl: string,
  serviceName: string
): Promise<ServiceDefinition | null> {
  try {
    const response = await callReflection(serverUrl, {
      fileContainingSymbol: serviceName,
    });

    if (response.errorResponse) {
      console.warn(
        `Warning: Could not get definition for ${serviceName}: ${response.errorResponse.errorMessage}`
      );
      return null;
    }

    const parts = serviceName.split('.');
    const name = parts[parts.length - 1];
    const packageName = parts.slice(0, -1).join('.');

    return {
      name,
      fullName: serviceName,
      package: packageName,
      methods: [],
    };
  } catch (error) {
    console.warn(`Warning: Failed to get service definition for ${serviceName}`);
    return null;
  }
}

async function callReflection(
  serverUrl: string,
  request: Record<string, unknown>
): Promise<ReflectionResponse> {
  const url = `${serverUrl}/grpc.reflection.v1.ServerReflection/ServerReflectionInfo`;

  const requestBody = encodeReflectionRequest(request);

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/grpc-web+proto',
      'Accept': 'application/grpc-web+proto',
      'x-grpc-web': '1',
    },
    body: requestBody,
  });

  if (!response.ok) {
    throw new Error(
      `Reflection request failed: ${response.status} ${response.statusText}`
    );
  }

  const responseBody = await response.arrayBuffer();
  return decodeReflectionResponse(new Uint8Array(responseBody));
}

function encodeReflectionRequest(request: Record<string, unknown>): Uint8Array {
  const parts: number[] = [];

  if ('listServices' in request) {
    parts.push(0x3a, 0x00);
  } else if ('fileContainingSymbol' in request) {
    const symbol = request.fileContainingSymbol as string;
    const symbolBytes = new TextEncoder().encode(symbol);
    parts.push(0x22, symbolBytes.length, ...symbolBytes);
  }

  const message = new Uint8Array(parts);

  const frame = new Uint8Array(5 + message.length);
  frame[0] = 0x00;
  const len = message.length;
  frame[1] = (len >> 24) & 0xff;
  frame[2] = (len >> 16) & 0xff;
  frame[3] = (len >> 8) & 0xff;
  frame[4] = len & 0xff;
  frame.set(message, 5);

  return frame;
}

function decodeReflectionResponse(data: Uint8Array): ReflectionResponse {
  if (data.length < 5) {
    return {};
  }

  const messageLength =
    (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];
  const message = data.slice(5, 5 + messageLength);

  const result: ReflectionResponse = {};

  let offset = 0;
  while (offset < message.length) {
    const fieldTag = message[offset];
    const fieldNumber = fieldTag >> 3;
    const wireType = fieldTag & 0x07;

    offset++;

    if (fieldNumber === 6 && wireType === 2) {
      const length = message[offset++];
      const submessage = message.slice(offset, offset + length);
      offset += length;

      const services: Array<{ name: string }> = [];
      let subOffset = 0;
      while (subOffset < submessage.length) {
        const subTag = submessage[subOffset++];
        const subFieldNumber = subTag >> 3;
        if (subFieldNumber === 1) {
          const nameLength = submessage[subOffset++];
          const nameBytes = submessage.slice(subOffset, subOffset + nameLength);
          subOffset += nameLength;
          services.push({ name: new TextDecoder().decode(nameBytes) });
        }
      }
      result.listServicesResponse = { service: services };
    } else if (wireType === 2) {
      const length = message[offset++];
      offset += length;
    } else if (wireType === 0) {
      while (message[offset] & 0x80) offset++;
      offset++;
    }
  }

  return result;
}
