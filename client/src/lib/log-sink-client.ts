// LogSink gRPC-Web client
import { GrpcWebFetchTransport } from '@protobuf-ts/grpcweb-transport';
import { LogSinkClient } from '../generated/sinks/log.client';
import type {
	QueryLogsRequest,
	QueryLogsResponse,
	LogEntry,
	LogUpdate
} from '../generated/sinks/log';

export type { LogEntry, LogUpdate };

export interface QueryLogsResult {
	logs: LogEntry[];
	total: number;
}

/**
 * Log Sink gRPC-Web Client
 *
 * This client connects to the torii.sinks.log.LogSink service
 * which provides log querying and real-time subscriptions.
 *
 * Features:
 * - QueryLogs: Get recent logs with optional limit
 * - SubscribeLogs: Subscribe to real-time log updates
 */
export class ToriiLogSinkClient {
	private client: LogSinkClient;
	private baseUrl: string;

	constructor(baseUrl: string = 'http://localhost:8080') {
		this.baseUrl = baseUrl;

		console.log('🔌 LogSinkClient connecting to:', baseUrl);

		// Create gRPC-Web transport (same as main Torii client)
		const transport = new GrpcWebFetchTransport({
			baseUrl,
			format: 'binary',
			interceptors: [
				{
					interceptUnary(next, method, input, options) {
						console.log('🟩 LogSink unary call:', method.name);
						return next(method, input, options);
					},
					interceptServerStreaming(next, method, input, options) {
						console.log('🟩 LogSink streaming call:', method.name);
						return next(method, input, options);
					}
				}
			]
		});

		this.client = new LogSinkClient(transport);
		console.log('✅ LogSink gRPC-Web client initialized');
	}

	/**
	 * Query recent logs
	 * @param limit Max number of logs to return (default: 5)
	 */
	async queryLogs(limit: number = 5): Promise<QueryLogsResult> {
		try {
			console.log('📋 Querying logs with limit:', limit);

			const request: QueryLogsRequest = {
				limit
			};

			const { response } = await this.client.queryLogs(request);

			console.log(`✅ Query returned ${response.logs.length} logs`);
			if (response.logs.length > 0) {
				console.log('📋 Sample log:', response.logs[0]);
			}

			return {
				logs: response.logs,
				total: response.logs.length
			};
		} catch (error: any) {
			console.error('❌ Log query failed:', error);
			console.error('Error details:', {
				name: error.name,
				message: error.message,
				code: error.code,
				stack: error.stack
			});
			throw new Error(`Log query failed: ${error.message}`);
		}
	}

	/**
	 * Subscribe to real-time log updates (server-side streaming)
	 * @param limit Max number of initial logs to fetch (default: 5)
	 * @param onUpdate Callback for each log update
	 * @param onError Error callback
	 * @param onConnected Connected callback
	 */
	async subscribeLogs(
		limit: number = 5,
		onUpdate: (update: LogUpdate) => void,
		onError: (error: Error) => void,
		onConnected: () => void
	): Promise<() => void> {
		try {
			console.log('📡 Starting LogSink subscription with limit:', limit);

			const request: QueryLogsRequest = {
				limit
			};

			const call = this.client.subscribeLogs(request);

			onConnected();
			console.log('✅ LogSink subscription connected!');

			// Handle incoming updates
			(async () => {
				try {
					for await (const update of call.responses) {
						console.log('📥 LogSink update received:', update);
						onUpdate(update);
					}
					console.log('🔚 LogSink subscription ended');
				} catch (err: any) {
					console.error('❌ LogSink subscription error:', err);
					onError(new Error(`Subscription error: ${err.message}`));
				}
			})();

			// Return unsubscribe function
			return () => {
				console.log('🛑 Canceling LogSink subscription');
				// Stream will close automatically
			};
		} catch (error: any) {
			console.error('❌ Failed to start LogSink subscription:', error);
			onError(error);
			return () => {};
		}
	}
}
