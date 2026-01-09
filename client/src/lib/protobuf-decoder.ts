// Protobuf Any decoder for sink messages
import { Any } from '../generated/google/protobuf/any';
import { SqlOperation } from '../generated/sinks/sql';
import { LogEntry } from '../generated/sinks/log';

/**
 * Type ID constants for known message types
 */
export const TYPE_IDS = {
	SQL_OPERATION: 'type.googleapis.com/torii.sinks.sql.SqlOperation',
	LOG_ENTRY: 'type.googleapis.com/torii.sinks.log.LogEntry',
} as const;

/**
 * Decode protobuf Any to a specific message type
 */
export function decodeAny(any: Any | undefined): DecodedMessage | null {
	if (!any || !any.type_url || !any.value) {
		return null;
	}

	try {
		switch (any.type_url) {
			case TYPE_IDS.SQL_OPERATION:
				return {
					typeUrl: any.type_url,
					typeName: 'SqlOperation',
					data: SqlOperation.fromBinary(any.value)
				};
			case TYPE_IDS.LOG_ENTRY:
				return {
					typeUrl: any.type_url,
					typeName: 'LogEntry',
					data: LogEntry.fromBinary(any.value)
				};
			default:
				console.warn('Unknown protobuf type:', any.type_url);
				return {
					typeUrl: any.type_url,
					typeName: 'Unknown',
					data: null
				};
		}
	} catch (error) {
		console.error('Failed to decode protobuf Any:', error);
		return null;
	}
}

/**
 * Format SqlOperation for display
 */
export function formatSqlOperation(operation: SqlOperation): any {
	return {
		table: operation.table,
		operation: operation.operation,
		value: operation.value.toString()
	};
}

/**
 * Format LogEntry for display
 */
export function formatLogEntry(log: LogEntry): any {
	return {
		id: log.id.toString(),
		message: log.message,
		timestamp: log.timestamp,
		blockNumber: log.block_number.toString(),
		eventKey: log.event_key
	};
}

export interface DecodedMessage {
	typeUrl: string;
	typeName: string;
	data: any;
}

