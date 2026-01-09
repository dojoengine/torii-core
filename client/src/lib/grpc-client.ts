// gRPC-Web client using @protobuf-ts (proper TypeScript, no CommonJS!)
import { GrpcWebFetchTransport } from '@protobuf-ts/grpcweb-transport';
import { ToriiClient } from '../generated/torii.client';
import type {
	SubscriptionRequest,
	TopicSubscription,
	TopicUpdate
} from '../generated/torii';
import type { Any } from '../generated/google/protobuf/any';

export type { TopicSubscription, TopicUpdate };

export interface EntityUpdate {
	updateType: number;
	topic: string;
	timestamp: number;
	// Protobuf fields
	typeId: string;
	data?: Any;
}

/**
 * Torii gRPC-Web Client using @protobuf-ts
 * Clean TypeScript with real server-side streaming!
 */
export class ToriiGrpcClient {
	private client: ToriiClient;
	private baseUrl: string;
	private abortController: AbortController | null = null;

	constructor(baseUrl: string = 'http://localhost:8080') {
		this.baseUrl = baseUrl;

		console.log('🔌 ToriiGrpcClient connecting to:', baseUrl);

		// Create gRPC-Web transport - matching Dojo's configuration
		// The format 'binary' uses application/grpc-web+proto content-type
		const transport = new GrpcWebFetchTransport({
			baseUrl,
			format: 'binary',
			// Add interceptors to log request/response details
			interceptors: [
				{
					interceptUnary(next, method, input, options) {
						console.log('🔵 Unary call:', method.name);
						return next(method, input, options);
					},
					interceptServerStreaming(next, method, input, options) {
						console.log('🔵 Server streaming call:', method.name);
						return next(method, input, options);
					}
				}
			]
		});

		this.client = new ToriiClient(transport);
		console.log('✅ gRPC-Web client initialized with @protobuf-ts');
	}

	/**
	 * Get server version via gRPC
	 */
	async getVersion(): Promise<{ version: string; buildTime: string }> {
		try {
			const { response } = await this.client.getVersion({});
			return {
				version: response.version,
				buildTime: response.build_time
			};
		} catch (error: any) {
			console.error('gRPC GetVersion error:', error);
			throw new Error(`gRPC GetVersion failed: ${error.message}`);
		}
	}

	/**
	 * List available topics via gRPC
	 */
	async listTopics(): Promise<Array<{
		name: string;
		sinkName: string;
		availableFilters: string[];
		description: string;
	}>> {
		try {
			console.log('📡 Calling listTopics...');
			const call = this.client.listTopics({});
			console.log('Call object:', call);

			const { response } = await call;
			console.log('✅ ListTopics response received:', response);

			return response.topics.map(topic => ({
				name: topic.name,
				sinkName: topic.sink_name,
				availableFilters: topic.available_filters,
				description: topic.description
			}));
		} catch (error: any) {
			console.error('❌ gRPC ListTopics error (full):', error);
			console.error('Error name:', error.name);
			console.error('Error message:', error.message);
			console.error('Error code:', error.code);
			console.error('Error stack:', error.stack);
			throw new Error(`gRPC ListTopics failed: ${error.message}`);
		}
	}

	/**
	 * Subscribe to topics using server-side streaming
	 * Real gRPC streaming with @protobuf-ts!
	 */
	async subscribeToTopics(
		clientId: string,
		topics: TopicSubscription[],
		unsubscribeTopics: string[] = [],
		onUpdate: (update: EntityUpdate) => void,
		onError: (error: Error) => void,
		onConnected: () => void
	): Promise<() => void> {
		try {
			this.abortController = new AbortController();

			const request: SubscriptionRequest = {
				client_id: clientId,
				topics: topics,
				unsubscribe_topics: unsubscribeTopics
			};

			console.log('📡 Starting gRPC server-side streaming...', request);

			// Server-side streaming call
			const call = this.client.subscribeToTopicsStream(request, {
				abort: this.abortController.signal
			});

			onConnected();
			console.log('✅ gRPC stream connected!');

			// Handle incoming messages
			(async () => {
				try {
					for await (const response of call.responses) {
						const update: EntityUpdate = {
							updateType: response.update_type,
							topic: response.topic,
							timestamp: Number(response.timestamp),
							typeId: response.type_id,
							data: response.data
						};

						console.log('📬 Received gRPC update:', update);
						onUpdate(update);
					}
					console.log('🔚 gRPC stream ended normally');
				} catch (err: any) {
					// Ignore abort errors and missing trailers (these happen during intentional disconnects)
					if (err.name === 'AbortError' || err.message?.includes('missing trailers')) {
						console.log('ℹ️ Stream closed gracefully');
						return;
					}
					console.error('❌ Stream error:', err);
					onError(new Error(`Stream error: ${err.message}`));
				}
			})();

			// Return unsubscribe function
			return () => {
				console.log('🛑 Canceling gRPC stream');
				if (this.abortController) {
					this.abortController.abort();
					this.abortController = null;
				}
			};
		} catch (error: any) {
			console.error('Failed to start gRPC subscription:', error);
			onError(error);
			return () => {};
		}
	}

	/**
	 * Get server health via HTTP
	 */
	async getHealth() {
		const response = await fetch(`${this.baseUrl}/health`);
		return response.json();
	}

	/**
	 * Disconnect any active streams
	 */
	disconnect() {
		if (this.abortController) {
			this.abortController.abort();
			this.abortController = null;
		}
	}
}
