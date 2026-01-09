import { generateClientCode, type ServiceDefinition } from './generator';
import { Glob } from 'bun';
import { resolve, basename, dirname, relative } from 'path';

interface ProtoMessage {
  name: string;
  fields: ProtoField[];
}

interface ProtoField {
  name: string;
  type: string;
  number: number;
  repeated: boolean;
  optional: boolean;
  mapKey?: string;
  mapValue?: string;
}

interface ProtoMethod {
  name: string;
  inputType: string;
  outputType: string;
  clientStreaming: boolean;
  serverStreaming: boolean;
}

interface ProtoService {
  name: string;
  methods: ProtoMethod[];
}

interface ParsedProto {
  package: string;
  services: ProtoService[];
  messages: ProtoMessage[];
}

export async function generateFromProtos(
  protoPath: string,
  outputDir: string
): Promise<void> {
  console.log('Searching for proto files...');

  const protos = await discoverProtos(protoPath);
  console.log(`Found ${protos.length} proto files:`);
  protos.forEach((p) => console.log(`  - ${relative(protoPath, p)}`));

  if (protos.length === 0) {
    throw new Error('No .proto files found in the specified path');
  }

  const serviceDefinitions: ServiceDefinition[] = [];

  for (const protoFile of protos) {
    const parsed = await parseProtoFile(protoFile);

    for (const service of parsed.services) {
      serviceDefinitions.push({
        name: service.name,
        fullName: parsed.package ? `${parsed.package}.${service.name}` : service.name,
        package: parsed.package,
        methods: service.methods.map((m) => ({
          name: m.name,
          inputType: m.inputType,
          outputType: m.outputType,
          clientStreaming: m.clientStreaming,
          serverStreaming: m.serverStreaming,
        })),
      });
    }
  }

  console.log(`\nParsed ${serviceDefinitions.length} services from proto files`);

  await generateClientCode(serviceDefinitions, outputDir);
}

async function discoverProtos(basePath: string): Promise<string[]> {
  const glob = new Glob('**/*.proto');
  const protos: string[] = [];

  const absolutePath = resolve(basePath);

  for await (const file of glob.scan({ cwd: absolutePath, absolute: true })) {
    protos.push(file);
  }

  return protos;
}

async function parseProtoFile(filePath: string): Promise<ParsedProto> {
  const content = await Bun.file(filePath).text();

  const result: ParsedProto = {
    package: '',
    services: [],
    messages: [],
  };

  const packageMatch = content.match(/package\s+([\w.]+)\s*;/);
  if (packageMatch) {
    result.package = packageMatch[1];
  }

  const serviceRegex = /service\s+(\w+)\s*\{([^}]*)\}/gs;
  let serviceMatch;

  while ((serviceMatch = serviceRegex.exec(content)) !== null) {
    const serviceName = serviceMatch[1];
    const serviceBody = serviceMatch[2];

    const methods: ProtoMethod[] = [];
    const rpcRegex =
      /rpc\s+(\w+)\s*\(\s*(stream\s+)?(\w+)\s*\)\s*returns\s*\(\s*(stream\s+)?(\w+)\s*\)/g;
    let rpcMatch;

    while ((rpcMatch = rpcRegex.exec(serviceBody)) !== null) {
      methods.push({
        name: rpcMatch[1],
        clientStreaming: !!rpcMatch[2],
        inputType: rpcMatch[3],
        serverStreaming: !!rpcMatch[4],
        outputType: rpcMatch[5],
      });
    }

    result.services.push({
      name: serviceName,
      methods,
    });
  }

  const messageRegex = /message\s+(\w+)\s*\{([^}]*)\}/gs;
  let messageMatch;

  while ((messageMatch = messageRegex.exec(content)) !== null) {
    const messageName = messageMatch[1];
    const messageBody = messageMatch[2];

    const fields: ProtoField[] = [];
    const fieldRegex =
      /(repeated\s+)?(optional\s+)?(?:map<(\w+),\s*(\w+)>|(\w+))\s+(\w+)\s*=\s*(\d+)/g;
    let fieldMatch;

    while ((fieldMatch = fieldRegex.exec(messageBody)) !== null) {
      const isRepeated = !!fieldMatch[1];
      const isOptional = !!fieldMatch[2];
      const mapKey = fieldMatch[3];
      const mapValue = fieldMatch[4];
      const type = fieldMatch[5] || `map<${mapKey}, ${mapValue}>`;
      const name = fieldMatch[6];
      const number = parseInt(fieldMatch[7], 10);

      fields.push({
        name,
        type,
        number,
        repeated: isRepeated,
        optional: isOptional,
        mapKey,
        mapValue,
      });
    }

    result.messages.push({
      name: messageName,
      fields,
    });
  }

  return result;
}
