import { writeFile } from 'fs/promises';
import { join } from 'path';
import type { FileDescriptorProto } from '@bufbuild/protobuf/wkt';

export interface MessageDefinition {
  name: string;
  fullName: string;
  fields: FieldDefinition[];
  nestedTypes: MessageDefinition[];
  nestedEnums: EnumDefinition[];
  isMapEntry?: boolean;
}

export interface FieldDefinition {
  name: string;
  jsonName: string;
  number: number;
  tsType: string;
  protoType: number;
  repeated: boolean;
  optional: boolean;
  isMap: boolean;
  mapKeyType?: string;
  mapValueType?: string;
  messageTypeName?: string;
  enumTypeName?: string;
}

export interface EnumDefinition {
  name: string;
  fullName: string;
  values: { name: string; number: number }[];
}

// Proto type constants
const TYPE_DOUBLE = 1;
const TYPE_FLOAT = 2;
const TYPE_INT64 = 3;
const TYPE_UINT64 = 4;
const TYPE_INT32 = 5;
const TYPE_FIXED64 = 6;
const TYPE_FIXED32 = 7;
const TYPE_BOOL = 8;
const TYPE_STRING = 9;
const TYPE_GROUP = 10;
const TYPE_MESSAGE = 11;
const TYPE_BYTES = 12;
const TYPE_UINT32 = 13;
const TYPE_ENUM = 14;
const TYPE_SFIXED32 = 15;
const TYPE_SFIXED64 = 16;
const TYPE_SINT32 = 17;
const TYPE_SINT64 = 18;

// Label constants
const LABEL_REPEATED = 3;

export function extractTypesFromFileDescriptor(
  fd: FileDescriptorProto,
  allMessages: Map<string, MessageDefinition>,
  allEnums: Map<string, EnumDefinition>
): void {
  const pkg = fd.package || '';

  // Extract top-level enums
  for (const enumType of fd.enumType) {
    const enumDef = extractEnum(enumType, pkg);
    allEnums.set(enumDef.fullName, enumDef);
  }

  // Extract top-level messages
  for (const msgType of fd.messageType) {
    extractMessage(msgType, pkg, allMessages, allEnums);
  }
}

function extractEnum(enumType: any, parentFullName: string): EnumDefinition {
  const fullName = parentFullName ? `${parentFullName}.${enumType.name}` : enumType.name;
  return {
    name: enumType.name,
    fullName,
    values: (enumType.value || []).map((v: any) => ({
      name: v.name,
      number: v.number,
    })),
  };
}

function extractMessage(
  msgType: any,
  parentFullName: string,
  allMessages: Map<string, MessageDefinition>,
  allEnums: Map<string, EnumDefinition>
): MessageDefinition {
  const fullName = parentFullName ? `${parentFullName}.${msgType.name}` : msgType.name;

  // Check if this is a map entry
  const isMapEntry = msgType.options?.mapEntry === true;

  const nestedEnums: EnumDefinition[] = [];
  for (const nestedEnum of msgType.enumType || []) {
    const enumDef = extractEnum(nestedEnum, fullName);
    nestedEnums.push(enumDef);
    allEnums.set(enumDef.fullName, enumDef);
  }

  const nestedTypes: MessageDefinition[] = [];
  for (const nestedMsg of msgType.nestedType || []) {
    const nested = extractMessage(nestedMsg, fullName, allMessages, allEnums);
    nestedTypes.push(nested);
  }

  const fields: FieldDefinition[] = [];
  for (const field of msgType.field || []) {
    fields.push(extractField(field, fullName, nestedTypes));
  }

  const msgDef: MessageDefinition = {
    name: msgType.name,
    fullName,
    fields,
    nestedTypes,
    nestedEnums,
    isMapEntry,
  };

  allMessages.set(fullName, msgDef);
  return msgDef;
}

function extractField(field: any, parentFullName: string, nestedTypes: MessageDefinition[]): FieldDefinition {
  const repeated = field.label === LABEL_REPEATED;
  const optional = field.proto3Optional === true;

  let tsType = protoTypeToTs(field.type);
  let messageTypeName: string | undefined;
  let enumTypeName: string | undefined;
  let isMap = false;
  let mapKeyType: string | undefined;
  let mapValueType: string | undefined;

  if (field.type === TYPE_MESSAGE || field.type === TYPE_ENUM) {
    const typeName = field.typeName?.replace(/^\./, '') || '';
    if (field.type === TYPE_MESSAGE) {
      messageTypeName = typeName;
      // Check if this is a map field
      const nestedMapEntry = nestedTypes.find(
        (n) => n.fullName === typeName && n.isMapEntry
      );
      if (nestedMapEntry && repeated) {
        isMap = true;
        const keyField = nestedMapEntry.fields.find((f) => f.name === 'key');
        const valueField = nestedMapEntry.fields.find((f) => f.name === 'value');
        mapKeyType = keyField?.tsType || 'string';
        mapValueType = valueField?.messageTypeName
          ? getShortTypeName(valueField.messageTypeName)
          : valueField?.tsType || 'unknown';
        tsType = `Record<${mapKeyType}, ${mapValueType}>`;
      } else {
        tsType = getShortTypeName(typeName);
      }
    } else {
      enumTypeName = typeName;
      tsType = getShortTypeName(typeName);
    }
  }

  if (repeated && !isMap) {
    tsType = `${tsType}[]`;
  }

  return {
    name: field.name,
    jsonName: field.jsonName || snakeToCamel(field.name),
    number: field.number,
    tsType,
    protoType: field.type,
    repeated: repeated && !isMap,
    optional,
    isMap,
    mapKeyType,
    mapValueType,
    messageTypeName,
    enumTypeName,
  };
}

function protoTypeToTs(protoType: number): string {
  switch (protoType) {
    case TYPE_DOUBLE:
    case TYPE_FLOAT:
    case TYPE_INT32:
    case TYPE_UINT32:
    case TYPE_SINT32:
    case TYPE_FIXED32:
    case TYPE_SFIXED32:
      return 'number';
    case TYPE_INT64:
    case TYPE_UINT64:
    case TYPE_SINT64:
    case TYPE_FIXED64:
    case TYPE_SFIXED64:
      return 'bigint';
    case TYPE_BOOL:
      return 'boolean';
    case TYPE_STRING:
      return 'string';
    case TYPE_BYTES:
      return 'Uint8Array';
    case TYPE_MESSAGE:
    case TYPE_GROUP:
      return 'unknown';
    case TYPE_ENUM:
      return 'number';
    default:
      return 'unknown';
  }
}

function protoTypeToString(protoType: number): string {
  switch (protoType) {
    case TYPE_DOUBLE: return 'double';
    case TYPE_FLOAT: return 'float';
    case TYPE_INT64: return 'int64';
    case TYPE_UINT64: return 'uint64';
    case TYPE_INT32: return 'int32';
    case TYPE_FIXED64: return 'fixed64';
    case TYPE_FIXED32: return 'fixed32';
    case TYPE_BOOL: return 'bool';
    case TYPE_STRING: return 'string';
    case TYPE_GROUP: return 'group';
    case TYPE_MESSAGE: return 'message';
    case TYPE_BYTES: return 'bytes';
    case TYPE_UINT32: return 'uint32';
    case TYPE_ENUM: return 'enum';
    case TYPE_SFIXED32: return 'sfixed32';
    case TYPE_SFIXED64: return 'sfixed64';
    case TYPE_SINT32: return 'sint32';
    case TYPE_SINT64: return 'sint64';
    default: return 'unknown';
  }
}

function getShortTypeName(fullName: string): string {
  const parts = fullName.split('.');
  return parts[parts.length - 1];
}

function snakeToCamel(str: string): string {
  return str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
}

export async function generateTypesFile(
  messages: Map<string, MessageDefinition>,
  enums: Map<string, EnumDefinition>,
  outputDir: string
): Promise<void> {
  const lines: string[] = ['// Generated by torii.js CLI', '// TypeScript interfaces for protobuf messages', ''];

  // Generate enums
  for (const [, enumDef] of enums) {
    lines.push(`export enum ${enumDef.name} {`);
    for (const value of enumDef.values) {
      lines.push(`  ${value.name} = ${value.number},`);
    }
    lines.push('}');
    lines.push('');
  }

  // Generate interfaces (skip map entry types)
  for (const [, msgDef] of messages) {
    if (msgDef.isMapEntry) continue;

    lines.push(`export interface ${msgDef.name} {`);
    for (const field of msgDef.fields) {
      const optional = field.optional ? '?' : '';
      lines.push(`  ${field.jsonName}${optional}: ${field.tsType};`);
    }
    lines.push('}');
    lines.push('');
  }

  await writeFile(join(outputDir, 'types.ts'), lines.join('\n'));
  console.log('  Generated: types.ts');
}

export async function generateSchemaFile(
  messages: Map<string, MessageDefinition>,
  enums: Map<string, EnumDefinition>,
  outputDir: string
): Promise<void> {
  const lines: string[] = [
    '// Generated by torii.js CLI',
    '// Field metadata for protobuf encoding/decoding',
    '',
    'export interface FieldSchema {',
    '  number: number;',
    '  type: string;',
    '  repeated: boolean;',
    '  optional?: boolean;',
    '  messageType?: string;',
    '  enumType?: string;',
    '  mapKey?: string;',
    '  mapValue?: string;',
    '}',
    '',
    'export interface MessageSchema {',
    '  name: string;',
    '  fullName: string;',
    '  fields: Record<string, FieldSchema>;',
    '}',
    '',
    '// Schema registry',
    'export const schemas: Record<string, MessageSchema> = {};',
    '',
  ];

  // Generate schemas for each message
  for (const [fullName, msgDef] of messages) {
    if (msgDef.isMapEntry) continue;

    const schemaName = `${msgDef.name}Schema`;
    lines.push(`export const ${schemaName}: MessageSchema = {`);
    lines.push(`  name: '${msgDef.name}',`);
    lines.push(`  fullName: '${fullName}',`);
    lines.push('  fields: {');
    for (const field of msgDef.fields) {
      lines.push(`    ${field.jsonName}: {`);
      lines.push(`      number: ${field.number},`);
      lines.push(`      type: '${protoTypeToString(field.protoType)}',`);
      lines.push(`      repeated: ${field.repeated},`);
      if (field.optional) {
        lines.push(`      optional: true,`);
      }
      if (field.messageTypeName) {
        lines.push(`      messageType: '${getShortTypeName(field.messageTypeName)}',`);
      }
      if (field.enumTypeName) {
        lines.push(`      enumType: '${getShortTypeName(field.enumTypeName)}',`);
      }
      if (field.isMap) {
        lines.push(`      mapKey: '${field.mapKeyType}',`);
        lines.push(`      mapValue: '${field.mapValueType}',`);
      }
      lines.push('    },');
    }
    lines.push('  },');
    lines.push('};');
    lines.push(`schemas['${msgDef.name}'] = ${schemaName};`);
    lines.push(`schemas['${fullName}'] = ${schemaName};`);
    lines.push('');
  }

  // Export helper to get schema by name
  lines.push('export function getSchema(name: string): MessageSchema | undefined {');
  lines.push('  return schemas[name];');
  lines.push('}');

  await writeFile(join(outputDir, 'schema.ts'), lines.join('\n'));
  console.log('  Generated: schema.ts');
}
