import { mkdir, writeFile } from 'fs/promises';
import { join } from 'path';
import type { MessageDefinition } from './types-generator';

export interface MethodDefinition {
  name: string;
  inputType: string;
  outputType: string;
  inputTypeShort?: string;
  outputTypeShort?: string;
  clientStreaming: boolean;
  serverStreaming: boolean;
}

export interface ServiceDefinition {
  name: string;
  fullName: string;
  package: string;
  methods: MethodDefinition[];
}

export interface GeneratorOptions {
  sdkImport: string;
}

const DEFAULT_OPTIONS: GeneratorOptions = {
  sdkImport: '@toriijs/sdk',
};

export async function generateClientCode(
  services: ServiceDefinition[],
  outputDir: string,
  options: Partial<GeneratorOptions> = {},
  messages?: Map<string, MessageDefinition>
): Promise<void> {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  await mkdir(outputDir, { recursive: true });

  // Deduplicate services by fullName
  const uniqueServices = new Map<string, ServiceDefinition>();
  for (const service of services) {
    if (!uniqueServices.has(service.fullName)) {
      uniqueServices.set(service.fullName, service);
    }
  }
  const deduplicatedServices = Array.from(uniqueServices.values());

  // Collect all types used by services
  const usedTypes = new Set<string>();
  for (const service of deduplicatedServices) {
    for (const method of service.methods) {
      if (method.inputTypeShort) usedTypes.add(method.inputTypeShort);
      if (method.outputTypeShort) usedTypes.add(method.outputTypeShort);
    }
  }

  for (const service of deduplicatedServices) {
    const clientCode = generateServiceClient(service, opts, messages);
    const fileName = `${service.name}Client.ts`;
    const filePath = join(outputDir, fileName);
    await writeFile(filePath, clientCode);
    console.log(`  Generated: ${fileName}`);
  }

  const indexCode = generateIndex(deduplicatedServices, messages);
  await writeFile(join(outputDir, 'index.ts'), indexCode);
  console.log(`  Generated: index.ts`);
}

function generateServiceClient(
  service: ServiceDefinition,
  options: GeneratorOptions,
  messages?: Map<string, MessageDefinition>
): string {
  const className = `${service.name}Client`;
  const hasTypes = messages && messages.size > 0;

  // Collect types and schemas used by this service
  const usedTypes = new Set<string>();
  const usedSchemas = new Set<string>();
  for (const method of service.methods) {
    if (method.inputTypeShort) {
      usedTypes.add(method.inputTypeShort);
      usedSchemas.add(`${method.inputTypeShort}Schema`);
    }
    if (method.outputTypeShort) {
      usedTypes.add(method.outputTypeShort);
      usedSchemas.add(`${method.outputTypeShort}Schema`);
    }
  }

  const methods = service.methods
    .map((method) => {
      const methodName = lowerFirst(method.name);
      const rpcPath = `/${service.fullName}/${method.name}`;
      const inputType = hasTypes && method.inputTypeShort ? method.inputTypeShort : 'Record<string, unknown>';
      const outputType = hasTypes && method.outputTypeShort ? method.outputTypeShort : 'Record<string, unknown>';
      const inputSchemaName = method.inputTypeShort ? `${method.inputTypeShort}Schema` : null;
      const outputSchemaName = method.outputTypeShort ? `${method.outputTypeShort}Schema` : null;

      // Build options object for schema-aware calls
      const schemaOptions = hasTypes && inputSchemaName && outputSchemaName
        ? `{ ...options, requestSchema: ${inputSchemaName}, responseSchema: ${outputSchemaName} }`
        : 'options';

      if (method.serverStreaming && !method.clientStreaming) {
        return `
  /**
   * ${method.name} - Server streaming RPC
   */
  async *${methodName}(
    request: ${inputType} = {} as ${inputType},
    options?: CallOptions
  ): AsyncGenerator<${outputType}> {
    yield* this.streamCall<${outputType}>('${rpcPath}', request as Record<string, unknown>, ${schemaOptions});
  }

  /**
   * ${method.name} with callbacks - Server streaming RPC
   */
  async on${method.name}(
    request: ${inputType},
    onMessage: (message: ${outputType}) => void,
    onError?: (error: Error) => void,
    onConnected?: () => void
  ): Promise<() => void> {
    return this._subscribeWithCallbacks<${outputType}>('${rpcPath}', request as Record<string, unknown>, onMessage, onError, onConnected, ${hasTypes && outputSchemaName ? outputSchemaName : 'undefined'});
  }`;
      } else if (method.clientStreaming && method.serverStreaming) {
        return `
  /**
   * ${method.name} - Bidirectional streaming RPC (not supported in gRPC-Web)
   */
  ${methodName}(): never {
    throw new Error('Bidirectional streaming not supported in gRPC-Web');
  }`;
      } else {
        return `
  /**
   * ${method.name} - Unary RPC
   */
  async ${methodName}(
    request: ${inputType} = {} as ${inputType},
    options?: CallOptions
  ): Promise<${outputType}> {
    return this.unaryCall<${outputType}>('${rpcPath}', request as Record<string, unknown>, ${schemaOptions});
  }`;
      }
    })
    .join('\n');

  // Build imports
  const imports: string[] = [`import { BaseSinkClient, setSchemaRegistry, type CallOptions } from '${options.sdkImport}';`];
  if (hasTypes && usedTypes.size > 0) {
    const typeImports = Array.from(usedTypes).join(', ');
    imports.push(`import type { ${typeImports} } from './types';`);
    const schemaImports = Array.from(usedSchemas).join(', ');
    imports.push(`import { ${schemaImports}, schemas } from './schema';`);
  }

  // Constructor to set up schema registry
  const constructor = hasTypes ? `
  constructor(baseUrl: string) {
    super(baseUrl);
    setSchemaRegistry(schemas);
  }
` : '';

  return `// Generated by torii.js CLI
// Service: ${service.fullName}

${imports.join('\n')}

export class ${className} extends BaseSinkClient {
${constructor}${methods}
}
`;
}

function generateIndex(services: ServiceDefinition[], messages?: Map<string, MessageDefinition>): string {
  const clientExports = services
    .map((s) => `export { ${s.name}Client } from './${s.name}Client';`)
    .join('\n');

  const lines = ['// Generated by torii.js CLI', clientExports];

  if (messages && messages.size > 0) {
    lines.push('');
    lines.push('// Re-export types');
    lines.push("export * from './types';");
    lines.push("export * from './schema';");
  }

  return lines.join('\n') + '\n';
}

function lowerFirst(str: string): string {
  return str.charAt(0).toLowerCase() + str.slice(1);
}
