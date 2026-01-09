#!/usr/bin/env bun
import { parseArgs } from 'util';
import { generateFromReflection } from './reflection';
import { generateFromProtos } from './protos';

interface CliOptions {
  url: string;
  output: string;
  sdkImport: string;
  path?: string;
}

function printHelp(): void {
  console.log(`
torii.js - TypeScript client generator for Torii gRPC services

Usage:
  torii.js [options]              Generate from gRPC reflection
  torii.js <path> [options]       Generate from proto files

Options:
  --url <url>           Server URL for reflection (default: http://localhost:8080)
  --output, -o          Output directory (default: ./generated)
  --sdk-import <path>   SDK import path (default: @toriijs/sdk)
  --help, -h            Show this help message

Examples:
  torii.js                                    # Reflection from localhost:8080
  torii.js --url http://myserver:8080         # Reflection from custom URL
  torii.js ./protos                           # Generate from proto files
  torii.js ./protos --output ./src/generated  # Custom output directory
  torii.js --sdk-import ../sdk                # Custom SDK import path
`);
}

function parseCliArgs(): CliOptions {
  const { values, positionals } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      url: {
        type: 'string',
        default: 'http://localhost:8080',
      },
      output: {
        type: 'string',
        short: 'o',
        default: './generated',
      },
      'sdk-import': {
        type: 'string',
        default: '@toriijs/sdk',
      },
      help: {
        type: 'boolean',
        short: 'h',
        default: false,
      },
    },
    allowPositionals: true,
  });

  if (values.help) {
    printHelp();
    process.exit(0);
  }

  return {
    url: values.url as string,
    output: values.output as string,
    sdkImport: values['sdk-import'] as string,
    path: positionals[0],
  };
}

async function main(): Promise<void> {
  const options = parseCliArgs();

  console.log('torii.js - gRPC Client Generator\n');

  try {
    if (options.path) {
      console.log(`Mode: Proto files`);
      console.log(`Path: ${options.path}`);
      console.log(`Output: ${options.output}`);
      console.log(`SDK Import: ${options.sdkImport}\n`);
      await generateFromProtos(options.path, options.output, {
        sdkImport: options.sdkImport,
      });
    } else {
      console.log(`Mode: gRPC Reflection`);
      console.log(`URL: ${options.url}`);
      console.log(`Output: ${options.output}`);
      console.log(`SDK Import: ${options.sdkImport}\n`);
      await generateFromReflection(options.url, options.output, {
        sdkImport: options.sdkImport,
      });
    }

    console.log('\nGeneration complete!');
  } catch (error) {
    console.error('\nError:', error instanceof Error ? error.message : error);
    process.exit(1);
  }
}

main();
