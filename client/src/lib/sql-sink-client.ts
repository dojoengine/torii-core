// SqlSink gRPC-Web client
import { GrpcWebFetchTransport } from '@protobuf-ts/grpcweb-transport';
import { SqlSinkClient } from '../generated/sinks/sql.client';
import type {
	QueryRequest,
	QueryRow,
	GetSchemaRequest,
	SqlSubscribeRequest,
	SqlOperationUpdate
} from '../generated/sinks/sql';

export type { QueryRow };

export interface QueryResult {
	rows: Array<{ [key: string]: string }>;
	totalRows: number;
}

export interface TableSchema {
	name: string;
	columns: { [key: string]: string };
}

/**
 * SQL Sink gRPC-Web Client
 *
 * This client connects to the torii.sinks.sql.SqlSink service
 * which provides direct SQL query capabilities.
 *
 * Features:
 * - Query: Execute SQL queries (for small result sets)
 * - StreamQuery: Stream large result sets row-by-row
 * - GetSchema: Get database schema information
 */
export class ToriiSqlSinkClient {
	private client: SqlSinkClient;
	private baseUrl: string;

	constructor(baseUrl: string = 'http://localhost:8080') {
		this.baseUrl = baseUrl;

		console.log('🔌 SqlSinkClient connecting to:', baseUrl);

		// Create gRPC-Web transport (same as main Torii client)
		const transport = new GrpcWebFetchTransport({
			baseUrl,
			format: 'binary',
			interceptors: [
				{
					interceptUnary(next, method, input, options) {
						console.log('🟦 SqlSink unary call:', method.name);
						return next(method, input, options);
					},
					interceptServerStreaming(next, method, input, options) {
						console.log('🟦 SqlSink streaming call:', method.name);
						return next(method, input, options);
					}
				}
			]
		});

		this.client = new SqlSinkClient(transport);
		console.log('✅ SqlSink gRPC-Web client initialized');
	}

	/**
	 * Execute a SQL query and return all results
	 * Use this for small result sets (< 1000 rows)
	 */
	async query(query: string, limit?: number): Promise<QueryResult> {
		try {
			console.log('📊 Executing SQL query:', query);

			const request: QueryRequest = {
				query,
				limit: limit
			};

			const { response } = await this.client.query(request);

			console.log(`✅ Query returned ${response.total_rows} rows`);
			if (response.rows.length > 0) {
				console.log('📋 Sample row:', response.rows[0].columns);
			}

			return {
				rows: response.rows.map((row) => row.columns),
				totalRows: response.total_rows
			};
		} catch (error: any) {
			console.error('❌ SQL query failed:', error);
			console.error('Error details:', {
				name: error.name,
				message: error.message,
				code: error.code,
				stack: error.stack
			});
			throw new Error(`SQL query failed: ${error.message}`);
		}
	}

	/**
	 * Execute a SQL query and stream results row-by-row
	 * Use this for large result sets to avoid loading all data in memory
	 */
	async streamQuery(
		query: string,
		onRow: (row: { [key: string]: string }) => void,
		onComplete: () => void,
		onError: (error: Error) => void,
		limit?: number
	): Promise<() => void> {
		try {
			console.log('📊 Starting SQL streaming query:', query);

			const request: QueryRequest = {
				query,
				limit: limit
			};

			const call = this.client.streamQuery(request);
			console.log('🔌 Stream call created:', call);

			let rowCount = 0;

			// Handle incoming rows
			(async () => {
				try {
					console.log('👂 Listening for rows...');
					for await (const row of call.responses) {
						rowCount++;
						console.log(`📥 Row ${rowCount}:`, row.columns);
						onRow(row.columns);
					}
					console.log(`✅ SQL stream completed (${rowCount} rows)`);
					onComplete();
				} catch (err: any) {
					console.error('❌ SQL stream error:', err);
					console.error('Error details:', {
						name: err.name,
						message: err.message,
						code: err.code
					});
					onError(new Error(`SQL stream error: ${err.message}`));
				}
			})();

			// Return cancel function
			return () => {
				console.log('🛑 Canceling SQL stream');
				// Note: protobuf-ts doesn't expose abort directly, but closing responses will stop the stream
			};
		} catch (error: any) {
			console.error('❌ Failed to start SQL stream:', error);
			onError(error);
			return () => {};
		}
	}

	/**
	 * Get database schema information
	 * @param tableName Optional: filter by specific table
	 */
	async getSchema(tableName?: string): Promise<TableSchema[]> {
		try {
			console.log('🔍 Getting database schema...');

			const request: GetSchemaRequest = {
				table_name: tableName
			};

			const { response } = await this.client.getSchema(request);

			console.log(`✅ Schema returned ${response.tables.length} table(s)`);

			return response.tables.map((table) => ({
				name: table.name,
				columns: table.columns
			}));
		} catch (error: any) {
			console.error('❌ Get schema failed:', error);
			throw new Error(`Get schema failed: ${error.message}`);
		}
	}

	/**
	 * Subscribe to real-time SQL operations (persistent stream)
	 * This is a long-living subscription that pushes updates as ETL processes events
	 */
	async subscribe(
		clientId: string,
		onUpdate: (update: SqlOperationUpdate) => void,
		onError: (error: Error) => void,
		onConnected: () => void,
		tableFilter?: string,
		operationFilter?: string
	): Promise<() => void> {
		try {
			console.log('📡 Starting SqlSink subscription...');
			console.log(`Filters: table=${tableFilter}, operation=${operationFilter}`);

			const request: SqlSubscribeRequest = {
				client_id: clientId,
				table: tableFilter,
				operation: operationFilter
			};

			const call = this.client.subscribe(request);

			onConnected();
			console.log('✅ SqlSink subscription connected!');

			// Handle incoming updates
			(async () => {
				try {
					for await (const update of call.responses) {
						console.log('📥 SqlSink update received:', update);
						onUpdate(update);
					}
					console.log('🔚 SqlSink subscription ended');
				} catch (err: any) {
					console.error('❌ SqlSink subscription error:', err);
					onError(new Error(`Subscription error: ${err.message}`));
				}
			})();

			// Return unsubscribe function
			return () => {
				console.log('🛑 Canceling SqlSink subscription');
				// Stream will close automatically
			};
		} catch (error: any) {
			console.error('❌ Failed to start SqlSink subscription:', error);
			onError(error);
			return () => {};
		}
	}
}
