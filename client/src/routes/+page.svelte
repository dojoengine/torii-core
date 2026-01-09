<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { ToriiGrpcClient, type EntityUpdate } from '$lib/grpc-client';
	import { ToriiSqlSinkClient, type QueryRow, type TableSchema } from '$lib/sql-sink-client';
	import { ToriiLogSinkClient, type LogEntry, type LogUpdate } from '$lib/log-sink-client';
	import { decodeAny, formatSqlOperation, formatLogEntry } from '$lib/protobuf-decoder';
	import type { SqlOperation } from '$lib/../generated/sinks/sql';
	import type { LogEntry as ProtoLogEntry } from '$lib/../generated/sinks/log';

	// Server config
	const SERVER_URL = 'http://localhost:8080';
	const client = new ToriiGrpcClient(SERVER_URL);
	const sqlClient = new ToriiSqlSinkClient(SERVER_URL);
	const logClient = new ToriiLogSinkClient(SERVER_URL);

	// State
	let clientId = $state(`browser-${Math.random().toString(36).substring(7)}`);
	let connected = $state(false);
	let updates: EntityUpdate[] = $state([]);
	let error = $state('');
	let serverHealth = $state<any>(null);
	let unsubscribe: (() => void) | null = null;
	let connectionInfo = $state('Both clients initialized. Check DevTools Network tab for Connection ID.');

	// Multi-topic subscription management
	interface TopicWithFilters {
		topic: string;
		filters: Record<string, string>;
	}
	let activeTopics = $state<TopicWithFilters[]>([]);
	let newTopicName = $state('entities');
	let newTopicFilters = $state<Array<{ key: string; value: string }>>([]);
	let topicsToUnsubscribe = $state<string[]>([]);

	// Topics discovery
	interface TopicInfo {
		name: string;
		sinkName: string;
		availableFilters: string[];
		description: string;
	}
	let availableTopics = $state<TopicInfo[]>([]);
	let topicsLoading = $state(false);
	let topicsError = $state('');

	// SQL query form (HTTP)
	let sqlQuery = $state('SELECT * FROM sql_operation ORDER BY id DESC LIMIT 10');
	let sqlResults = $state<any[]>([]);
	let sqlError = $state('');
	let sqlLoading = $state(false);

	// SQL Sink gRPC service state
	let grpcSqlQuery = $state('SELECT * FROM sql_operation LIMIT 5');
	let grpcSqlResults = $state<Array<{ [key: string]: string }>>([]);
	let grpcSqlError = $state('');
	let grpcSqlLoading = $state(false);
	let sqlSchema = $state<TableSchema[]>([]);
	let schemaLoading = $state(false);
	let streamingRows = $state<Array<{ [key: string]: string }>>([]);
	let isStreaming = $state(false);

	// SQL Sink subscription state (persistent stream)
	let sqlSinkSubscribed = $state(false);
	let sqlSinkUpdates = $state<any[]>([]);
	let sqlSinkUnsubscribe: (() => void) | null = null;

	// Log Sink state
	let logLimit = $state(10);
	let logResults = $state<LogEntry[]>([]);
	let logError = $state('');
	let logLoading = $state(false);
	let logSinkSubscribed = $state(false);
	let logSinkUpdates = $state<LogUpdate[]>([]);
	let logSinkUnsubscribe: (() => void) | null = null;

	// External fetch for connection comparison
	let externalFetchResult = $state('');

	// Add a filter pair to the new topic
	function addFilter() {
		newTopicFilters = [...newTopicFilters, { key: '', value: '' }];
	}

	function removeFilter(index: number) {
		newTopicFilters = newTopicFilters.filter((_, i) => i !== index);
	}

	// Add topic to the list (doesn't subscribe yet)
	function addTopicToList() {
		if (!newTopicName.trim()) {
			error = 'Topic name cannot be empty';
			return;
		}

		// Build filters object
		const filters: Record<string, string> = {};
		for (const f of newTopicFilters) {
			if (f.key.trim() && f.value.trim()) {
				filters[f.key.trim()] = f.value.trim();
			}
		}

		// Check if topic already exists
		const existingIndex = activeTopics.findIndex((t) => t.topic === newTopicName);
		if (existingIndex >= 0) {
			// Update existing topic
			activeTopics[existingIndex] = { topic: newTopicName, filters };
			activeTopics = [...activeTopics];
		} else {
			// Add new topic
			activeTopics = [...activeTopics, { topic: newTopicName, filters }];
		}

		// Reset form
		newTopicName = '';
		newTopicFilters = [];
		error = '';
	}

	function removeTopicFromList(topicName: string) {
		activeTopics = activeTopics.filter((t) => t.topic !== topicName);
	}

	function toggleUnsubscribe(topicName: string) {
		if (topicsToUnsubscribe.includes(topicName)) {
			topicsToUnsubscribe = topicsToUnsubscribe.filter((t) => t !== topicName);
		} else {
			topicsToUnsubscribe = [...topicsToUnsubscribe, topicName];
		}
	}

	// Subscribe/Update subscription with all active topics
	async function applySubscription() {
		try {
			error = '';

			if (activeTopics.length === 0 && topicsToUnsubscribe.length === 0) {
				error = 'Add at least one topic to subscribe or select topics to unsubscribe';
				return;
			}

			// Disconnect existing stream before creating a new one
			if (unsubscribe) {
				console.log('🔄 Closing existing stream before update...');
				unsubscribe();
				unsubscribe = null;
				// Small delay to let the old stream clean up
				await new Promise((resolve) => setTimeout(resolve, 100));
			}

			// Filter out topics marked for unsubscribe from the active list
			// We DON'T want to send them in both subscribe AND unsubscribe
			const topicsToSend = activeTopics
				.filter((t) => !topicsToUnsubscribe.includes(t.topic))
				.map((t) => ({
					topic: t.topic,
					filters: t.filters
				}));

			console.log('🚀 Applying subscription...', {
				topics: topicsToSend,
				unsubscribe: topicsToUnsubscribe
			});

		unsubscribe = await client.subscribeToTopics(
			clientId,
			topicsToSend,
			topicsToUnsubscribe,
			(update: EntityUpdate) => {
				console.log('📬 Update received:', update);

				// Decode protobuf data if present
				if (update.data) {
					const decoded = decodeAny(update.data);
					console.log('🔓 Decoded protobuf:', decoded);

					if (decoded && decoded.typeName === 'SqlOperation') {
						const formatted = formatSqlOperation(decoded.data as SqlOperation);
						console.log('✨ Formatted SQL operation:', formatted);
						// Attach decoded data to update for display
						(update as any).decodedData = formatted;
						(update as any).decodedType = 'SqlOperation';
					} else if (decoded && decoded.typeName === 'LogEntry') {
						const formatted = formatLogEntry(decoded.data as ProtoLogEntry);
						console.log('✨ Formatted Log entry:', formatted);
						// Attach decoded data to update for display
						(update as any).decodedData = formatted;
						(update as any).decodedType = 'LogEntry';
					}
				}

				updates = [update, ...updates].slice(0, 50);
			},
				(err: Error) => {
					// Ignore "missing trailers" errors when we intentionally close streams
					if (err.message.includes('missing trailers')) {
						console.log('ℹ️ Stream closed (expected during update)');
						return;
					}
					error = `Subscription error: ${err.message}`;
					console.error('Subscription error:', err);
					connected = false;
				},
				() => {
					connected = true;
					// Remove unsubscribed topics from active list
					if (topicsToUnsubscribe.length > 0) {
						activeTopics = activeTopics.filter(
							(t) => !topicsToUnsubscribe.includes(t.topic)
						);
					}
					topicsToUnsubscribe = [];
					console.log('✅ Subscription applied!');
				}
			);
		} catch (err: any) {
			error = `Failed to apply subscription: ${err.message}`;
			console.error('Subscription error:', err);
			connected = false;
		}
	}

	function disconnect() {
		if (unsubscribe) {
			unsubscribe();
			unsubscribe = null;
		}
		connected = false;
		console.log('Disconnected from gRPC stream');
	}

	async function loadTopics() {
		try {
			topicsError = '';
			topicsLoading = true;
			availableTopics = [];

			const topics = await client.listTopics();
			availableTopics = topics;
			console.log('✅ Topics loaded:', topics);
		} catch (err: any) {
			topicsError = `Failed to load topics: ${err.message}`;
			console.error('Topics error:', err);
		} finally {
			topicsLoading = false;
		}
	}

	async function testGrpcMethods() {
		try {
			error = '';
			const version = await client.getVersion();
			console.log('✅ gRPC GetVersion:', version);
			alert(`gRPC Working!\nVersion: ${version.version}\nBuild Time: ${version.buildTime}`);
		} catch (err: any) {
			error = `gRPC test failed: ${err.message}`;
		}
	}

	async function healthCheck() {
		try {
			error = '';
			const health = await client.getHealth();
			serverHealth = health;
			console.log('Server health:', health);
		} catch (err: any) {
			error = `Health check failed: ${err.message}`;
			serverHealth = null;
		}
	}

	async function executeSqlQuery() {
		try {
			sqlError = '';
			sqlLoading = true;
			sqlResults = [];

			const response = await fetch(`${SERVER_URL}/sql/query`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ query: sqlQuery })
			});

			if (!response.ok) {
				const text = await response.text();
				throw new Error(`HTTP ${response.status}: ${text}`);
			}

			const data = await response.json();
			sqlResults = data.rows || [];
			console.log('✅ SQL query executed:', data);
		} catch (err: any) {
			sqlError = `Query failed: ${err.message}`;
			console.error('SQL query error:', err);
		} finally {
			sqlLoading = false;
		}
	}

	async function loadSqlEvents() {
		try {
			sqlError = '';
			sqlLoading = true;
			sqlResults = [];

			const response = await fetch(`${SERVER_URL}/sql/events`);

			if (!response.ok) {
				const text = await response.text();
				throw new Error(`HTTP ${response.status}: ${text}`);
			}

			const data = await response.json();
			sqlResults = data.rows || [];
			console.log('✅ SQL events loaded:', data);
		} catch (err: any) {
			sqlError = `Failed to load events: ${err.message}`;
			console.error('SQL events error:', err);
		} finally {
			sqlLoading = false;
		}
	}

	// SQL Sink gRPC functions
	async function executeGrpcSqlQuery() {
		try {
			grpcSqlError = '';
			grpcSqlLoading = true;
			grpcSqlResults = [];

			console.log('📊 Executing gRPC query:', grpcSqlQuery);
			const result = await sqlClient.query(grpcSqlQuery);
			grpcSqlResults = result.rows;
			console.log(`✅ gRPC SQL query executed: ${result.totalRows} rows`);
			console.log('📋 First row sample:', result.rows[0]);
		} catch (err: any) {
			grpcSqlError = `gRPC query failed: ${err.message}`;
			console.error('❌ gRPC SQL query error:', err);
		} finally {
			grpcSqlLoading = false;
		}
	}

	async function loadSchemaGrpc() {
		try {
			grpcSqlError = '';
			schemaLoading = true;

			const schema = await sqlClient.getSchema();
			sqlSchema = schema;
			console.log(`✅ Schema loaded: ${schema.length} tables`);
		} catch (err: any) {
			grpcSqlError = `Schema load failed: ${err.message}`;
			console.error('Schema load error:', err);
		} finally {
			schemaLoading = false;
		}
	}

	async function streamGrpcQuery() {
		try {
			grpcSqlError = '';
			isStreaming = true;
			streamingRows = [];

			console.log('🔄 Starting stream query...');

			await sqlClient.streamQuery(
				grpcSqlQuery,
				(row) => {
					console.log('📥 Received row:', row);
					streamingRows = [...streamingRows, row];
					console.log(`📊 Total streaming rows: ${streamingRows.length}`);
				},
				() => {
					isStreaming = false;
					console.log(`✅ Stream completed: ${streamingRows.length} rows`);
				},
				(error) => {
					console.error('❌ Stream error:', error);
					grpcSqlError = `Stream failed: ${error.message}`;
					isStreaming = false;
				}
			);
		} catch (err: any) {
			console.error('❌ Stream initiation error:', err);
			grpcSqlError = `Stream failed: ${err.message}`;
			isStreaming = false;
		}
	}

	// SQL Sink subscription functions
	async function subscribeSqlSink() {
		try {
			grpcSqlError = '';
			sqlSinkUnsubscribe = await sqlClient.subscribe(
				clientId,
				(update) => {
					console.log('📥 SQL Sink update:', update);
					sqlSinkUpdates = [update, ...sqlSinkUpdates].slice(0, 50); // Keep last 50
				},
				(error) => {
					grpcSqlError = `Subscription error: ${error.message}`;
					sqlSinkSubscribed = false;
				},
				() => {
					sqlSinkSubscribed = true;
					console.log('✅ SQL Sink subscription active');
				}
			);
		} catch (err: any) {
			grpcSqlError = `Failed to subscribe: ${err.message}`;
		}
	}

	function unsubscribeSqlSink() {
		if (sqlSinkUnsubscribe) {
			sqlSinkUnsubscribe();
			sqlSinkUnsubscribe = null;
		}
		sqlSinkSubscribed = false;
	}

	// Log Sink functions
	async function queryLogs() {
		try {
			logError = '';
			logLoading = true;
			logResults = [];

			const result = await logClient.queryLogs(logLimit);
			logResults = result.logs;
			console.log(`✅ Queried ${result.total} logs`);
		} catch (err: any) {
			logError = `Query failed: ${err.message}`;
			console.error('Log query error:', err);
		} finally {
			logLoading = false;
		}
	}

	async function subscribeLogSink() {
		try {
			logError = '';
			logSinkUnsubscribe = await logClient.subscribeLogs(
				logLimit,
				(update) => {
					console.log('📥 Log Sink update:', update);
					logSinkUpdates = [update, ...logSinkUpdates].slice(0, 50); // Keep last 50
				},
				(error) => {
					logError = `Subscription error: ${error.message}`;
					logSinkSubscribed = false;
				},
				() => {
					logSinkSubscribed = true;
					console.log('✅ Log Sink subscription active');
				}
			);
		} catch (err: any) {
			logError = `Failed to subscribe: ${err.message}`;
		}
	}

	function unsubscribeLogSink() {
		if (logSinkUnsubscribe) {
			logSinkUnsubscribe();
			logSinkUnsubscribe = null;
		}
		logSinkSubscribed = false;
	}

	// External fetch to demonstrate different connection
	async function fetchExternal() {
		try {
			externalFetchResult = '⏳ Fetching...';
			// Fetch from httpbin.org (a test API that returns request info)
			const response = await fetch('https://httpbin.org/get');
			const data = await response.json();
			externalFetchResult = `✅ External fetch complete! Origin: ${data.origin}`;
			console.log('🌍 External fetch response:', data);
		} catch (err: any) {
			externalFetchResult = `❌ Failed: ${err.message}`;
		}
	}

	function clearUpdates() {
		updates = [];
	}

	function getUpdateTypeName(type: number): string {
		switch (type) {
			case 0:
				return 'CREATED';
			case 1:
				return 'UPDATED';
			case 2:
				return 'DELETED';
			default:
				return 'UNKNOWN';
		}
	}

	onMount(() => {
		console.log('🌐 Torii gRPC-Web Client initialized');
		console.log('📡 Using real gRPC server-side streaming!');
		healthCheck();
	});

	onDestroy(() => {
		disconnect();
		unsubscribeSqlSink();
		unsubscribeLogSink();
	});
</script>

<main>
	<div class="container">
		<header>
			<h1>🌐 Torii gRPC-Web Client</h1>
			<p class="subtitle">Real gRPC Server-Side Streaming + HTTP REST</p>
		</header>

		{#if error}
			<div class="error">⚠️ {error}</div>
		{/if}

		<!-- Status Panel -->
		<section class="panel status-panel">
			<h2>📊 Status</h2>
			<div class="status-grid">
				<div class="stat">
					<div class="stat-label">gRPC Stream</div>
					<div class="stat-value" class:success={connected} class:inactive={!connected}>
						{connected ? '🟢 Connected' : '⚪ Not Connected'}
					</div>
				</div>
				<div class="stat">
					<div class="stat-label">Server</div>
					<div class="stat-value" class:success={serverHealth}>
						{serverHealth ? '🟢 Online' : '⚪ Checking...'}
					</div>
				</div>
				<div class="stat">
					<div class="stat-label">Client ID</div>
					<div class="stat-value client-id">{clientId}</div>
				</div>
				<div class="stat">
					<div class="stat-label">Connection</div>
					<div class="stat-value small">
						{connectionInfo}
					</div>
				</div>
				<div class="stat">
					<div class="stat-label">Subscribed Topics</div>
					<div class="stat-value small">
						{#if activeTopics.length > 0}
							{activeTopics.map((t) => t.topic).join(', ')}
						{:else}
							None
						{/if}
					</div>
				</div>
				<div class="stat">
					<div class="stat-label">Updates Received</div>
					<div class="stat-value">{updates.length}</div>
				</div>
			</div>
		</section>

		<div class="panels">
			<!-- Multi-Topic Subscription Panel -->
			<section class="panel">
				<h2>📡 gRPC Multi-Topic Subscription</h2>
				<p class="help">
					Add topics to your subscription list, then click Subscribe to start receiving updates.
					You can subscribe to multiple topics at once!
				</p>

				<!-- Add New Topic Form -->
				<div class="topic-form">
					<h3>➕ Add Topic:</h3>
					<div class="form-group">
						<label>Topic Name: <span class="required">*</span></label>
						<input
							bind:value={newTopicName}
							placeholder="sql, logs, etc."
							onkeydown={(e) => e.key === 'Enter' && addTopicToList()}
							class:input-error={error && error.includes('Topic name')}
						/>
						<p class="hint" style="margin-top: 0.5rem; color: #888;">
							💡 Try "sql" or "logs" - or use <button class="btn-tiny" onclick={loadTopics} style="display: inline; margin: 0 0.25rem;">Load Topics</button> to discover available topics below
						</p>
					</div>

					<!-- Filters -->
					<div class="filters-section">
						<div style="display: flex; justify-content: space-between; align-items: center;">
							<label>Filters (optional):</label>
							<button class="btn-tiny" onclick={addFilter}>+ Add Filter</button>
						</div>

						{#each newTopicFilters as filter, index}
							<div class="filter-pair">
								<input
									bind:value={filter.key}
									placeholder="key (e.g. type)"
									style="flex: 1;"
								/>
								<span>=</span>
								<input
									bind:value={filter.value}
									placeholder="value (e.g. player)"
									style="flex: 1;"
								/>
								<button class="btn-tiny btn-danger-tiny" onclick={() => removeFilter(index)}>
									✕
								</button>
							</div>
						{/each}

						{#if newTopicFilters.length === 0}
							<p class="hint">No filters = subscribe to all updates</p>
						{/if}
					</div>

					<button class="btn-primary" onclick={addTopicToList}>
						➕ Add to Subscription List
					</button>
				</div>

				<!-- Active Topics List -->
				{#if activeTopics.length === 0}
					<div class="empty-topics-state">
						<p style="margin: 0; color: #666;">
							📋 No topics added yet
						</p>
						<p class="hint" style="margin: 0.5rem 0 0 0;">
							Add topics above using the form, then click Subscribe
						</p>
					</div>
				{:else}
					<div class="active-topics">
						<h3>📋 Subscription List ({activeTopics.length} topic{activeTopics.length === 1 ? '' : 's'}):</h3>
						<p class="hint" style="margin: 0 0 1rem 0;">
							Click <strong>Subscribe</strong> below to start receiving updates for these topics
						</p>
						<div class="topics-list">
							{#each activeTopics as topic}
								<div class="topic-item">
									<div class="topic-info">
										<strong>{topic.topic}</strong>
										{#if Object.keys(topic.filters).length > 0}
											<div class="topic-filters">
												{#each Object.entries(topic.filters) as [key, value]}
													<span class="filter-tag">{key}={value}</span>
												{/each}
											</div>
										{:else}
											<span class="no-filters">all updates</span>
										{/if}
									</div>
									<div class="topic-actions">
										<label class="unsub-checkbox">
											<input
												type="checkbox"
												checked={topicsToUnsubscribe.includes(topic.topic)}
												onchange={() => toggleUnsubscribe(topic.topic)}
											/>
											<span>Unsubscribe</span>
										</label>
										<button
											class="btn-tiny btn-danger-tiny"
											onclick={() => removeTopicFromList(topic.topic)}
										>
											Remove
										</button>
									</div>
								</div>
							{/each}
						</div>
					</div>
				{/if}

				<!-- Apply Subscription Button -->
				<div class="button-group" style="margin-top: 1.5rem;">
					<button
						class="btn-primary btn-large"
						onclick={applySubscription}
						disabled={activeTopics.length === 0 && topicsToUnsubscribe.length === 0}
					>
						{connected ? '🔄 Update Subscription' : '🚀 Subscribe'}
					</button>
					{#if connected}
						<button class="btn-danger" onclick={disconnect}>Disconnect</button>
					{/if}
					<button onclick={testGrpcMethods} class="btn-small">Test gRPC</button>
				</div>

				{#if topicsToUnsubscribe.length > 0}
					<div class="unsub-notice">
						⚠️ Will unsubscribe from: {topicsToUnsubscribe.join(', ')}
					</div>
				{/if}
			</section>

			<!-- Real-time Updates Display (Moved here to be next to subscription) -->
			<section class="panel">
				<div class="panel-header">
					<h2>📬 Real-time Updates ({updates.length})</h2>
					<button class="btn-small" onclick={clearUpdates}>Clear</button>
				</div>

				{#if updates.length === 0}
					<div class="empty-state">
						{#if connected}
							<p>✅ Subscribed and waiting for updates...</p>
							<p class="hint">Updates will appear here as events are processed</p>
						{:else}
							<p>Subscribe to topics to start receiving real-time updates</p>
						{/if}
					</div>
				{:else}
					<div class="updates-list">
						{#each updates as update, i (i)}
						<div class="update-item">
							<div class="update-header">
								<span class="update-type type-{update.updateType}">
									{getUpdateTypeName(update.updateType)}
								</span>
								<span class="update-topic">{update.topic}</span>
								{#if update.typeId}
									<span class="type-badge">{update.typeId}</span>
								{/if}
								<span class="update-time">
									{new Date(update.timestamp * 1000).toLocaleTimeString()}
								</span>
							</div>
							<div class="update-body">
								{#if (update as any).decodedData}
									<!-- Show decoded protobuf data -->
									<div class="protobuf-data">
										{#if (update as any).decodedType === 'SqlOperation'}
											<div class="data-label">🔓 Decoded: SqlOperation</div>
											<div class="data-grid">
												<div class="data-row">
													<span class="data-key">Table:</span>
													<span class="data-value">{(update as any).decodedData.table}</span>
												</div>
												<div class="data-row">
													<span class="data-key">Operation:</span>
													<span class="data-value operation-badge {(update as any).decodedData.operation}">
														{(update as any).decodedData.operation}
													</span>
												</div>
												<div class="data-row">
													<span class="data-key">Value:</span>
													<span class="data-value">{(update as any).decodedData.value}</span>
												</div>
											</div>
										{:else if (update as any).decodedType === 'LogEntry'}
											<div class="data-label">🔓 Decoded: LogEntry</div>
											<div class="data-grid">
												<div class="data-row">
													<span class="data-key">ID:</span>
													<span class="data-value">{(update as any).decodedData.id}</span>
												</div>
												<div class="data-row">
													<span class="data-key">Message:</span>
													<span class="data-value">{(update as any).decodedData.message}</span>
												</div>
												<div class="data-row">
													<span class="data-key">Block:</span>
													<span class="data-value">{(update as any).decodedData.blockNumber}</span>
												</div>
												<div class="data-row">
													<span class="data-key">Event Key:</span>
													<span class="data-value" style="font-family: monospace; font-size: 0.8rem; word-break: break-all;">
														{(update as any).decodedData.eventKey}
													</span>
												</div>
											</div>
										{/if}
									</div>
								{:else if update.data}
									<!-- Show raw protobuf data if not decoded -->
									<div class="protobuf-data">
										<div class="data-label">🔒 Raw Protobuf Data</div>
										<div class="data-grid">
											<div class="data-row">
												<span class="data-key">Type URL:</span>
												<span class="data-value" style="font-size: 0.85rem; word-break: break-all;">
													{update.data.type_url || 'unknown'}
												</span>
											</div>
											<div class="data-row">
												<span class="data-key">Data Size:</span>
												<span class="data-value">
													{update.data.value?.length || 0} bytes
												</span>
											</div>
										</div>
										<p class="hint" style="margin-top: 0.5rem; color: #999;">
											Decoder not available for this data type
										</p>
									</div>
								{:else}
									<!-- No data available -->
									<div class="empty-state" style="padding: 1rem;">
										<p style="margin: 0; color: #999; font-size: 0.9rem;">
											No data payload in this update
										</p>
									</div>
								{/if}
							</div>
						</div>
						{/each}
					</div>
				{/if}
			</section>

			<!-- Topics Discovery Panel -->
			<section class="panel">
				<h2>📋 Available Topics</h2>
				<p class="help">Discover available subscription topics and their filters</p>

				{#if topicsError}
					<div class="error" style="margin-bottom: 1rem;">⚠️ {topicsError}</div>
				{/if}

				<button
					class="btn-primary"
					onclick={loadTopics}
					disabled={topicsLoading}
				>
					{topicsLoading ? 'Loading...' : 'Load Topics'}
				</button>

				{#if availableTopics.length > 0}
					<div class="topics-discovery">
						<h3>Topics ({availableTopics.length}):</h3>
						{#each availableTopics as topic}
							<div class="topic-card">
								<div class="topic-header">
									<h4>{topic.name}</h4>
								</div>
								<p class="topic-description">{topic.description}</p>
								<div class="topic-filters-info">
									<strong>Available Filters:</strong>
									{#if topic.availableFilters.length > 0}
										<div class="filter-tags">
											{#each topic.availableFilters as filter}
												<span class="filter-tag">{filter}</span>
											{/each}
										</div>
									{:else}
										<span class="no-filters">No filters available</span>
									{/if}
								</div>
							</div>
						{/each}
					</div>
				{/if}
			</section>

			<!-- SQL Sink gRPC Service -->
			<section class="panel">
				<h2>🔷 SQL Sink gRPC Service</h2>
				<p class="help">
					Direct SQL queries via gRPC service (torii.sinks.sql.SqlSink)
				</p>

				{#if grpcSqlError}
					<div class="error" style="margin-bottom: 1rem;">⚠️ {grpcSqlError}</div>
				{/if}

				<div class="form-group">
					<label>SQL Query (gRPC):</label>
					<textarea
						bind:value={grpcSqlQuery}
						placeholder="SELECT * FROM sql_operation LIMIT 5"
						rows="3"
					></textarea>
				</div>

				<div style="display: flex; gap: 0.5rem; margin-bottom: 1rem; flex-wrap: wrap;">
					<button
						class="btn-primary"
						onclick={executeGrpcSqlQuery}
						disabled={grpcSqlLoading}
					>
						{grpcSqlLoading ? 'Executing...' : '📊 Query (Unary RPC)'}
					</button>

					<button
						class="btn-secondary"
						onclick={streamGrpcQuery}
						disabled={isStreaming}
					>
						{isStreaming ? 'Streaming...' : '📡 Stream Query (Server Streaming)'}
					</button>

					<button
						class="btn-secondary"
						onclick={loadSchemaGrpc}
						disabled={schemaLoading}
					>
						{schemaLoading ? 'Loading...' : '🔍 Get Schema'}
					</button>

					<button
						class="btn-danger"
						onclick={() => {
							grpcSqlResults = [];
							streamingRows = [];
							sqlSchema = [];
							grpcSqlError = '';
						}}
					>
						🗑️ Clear
					</button>
				</div>

				{#if grpcSqlResults.length > 0}
					{@const columns = Object.keys(grpcSqlResults[0]).sort()}
					<div class="result-section">
						<h3>Query Results ({grpcSqlResults.length} rows):</h3>
						<div class="table-container">
							<table>
								<thead>
									<tr>
										{#each columns as col}
											<th>{col}</th>
										{/each}
									</tr>
								</thead>
								<tbody>
									{#each grpcSqlResults as row}
										<tr>
											{#each columns as col}
												<td>{row[col] ?? ''}</td>
											{/each}
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{/if}

				{#if streamingRows.length > 0}
					{@const streamColumns = Object.keys(streamingRows[0]).sort()}
					<div class="result-section">
						<h3>
							Streaming Results ({streamingRows.length} rows)
							{#if isStreaming}
								<span class="streaming-indicator">● Streaming...</span>
							{/if}
						</h3>
						<div class="table-container">
							<table>
								<thead>
									<tr>
										{#each streamColumns as col}
											<th>{col}</th>
										{/each}
									</tr>
								</thead>
								<tbody>
									{#each streamingRows as row}
										<tr>
											{#each streamColumns as col}
												<td>{row[col] ?? ''}</td>
											{/each}
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{/if}

				{#if sqlSchema.length > 0}
					<div class="result-section">
						<h3>Database Schema ({sqlSchema.length} tables):</h3>
						{#each sqlSchema as table}
							<div class="schema-table">
								<h4>{table.name}</h4>
								<div class="schema-columns">
									{#each Object.entries(table.columns) as [colName, colType]}
										<div class="schema-column">
											<span class="col-name">{colName}</span>
											<span class="col-type">{colType}</span>
										</div>
									{/each}
								</div>
							</div>
						{/each}
					</div>
				{/if}
			</section>

			<!-- SQL Sink Persistent Subscription -->
			<section class="panel">
				<h2>🔔 SQL Sink Persistent Subscription (NEW!)</h2>
				<p class="help">
					Subscribe to real-time SQL operations as they happen (persistent stream)
				</p>

				{#if grpcSqlError && sqlSinkSubscribed}
					<div class="error" style="margin-bottom: 1rem;">⚠️ {grpcSqlError}</div>
				{/if}

				<div style="display: flex; gap: 0.5rem; margin-bottom: 1rem;">
					{#if !sqlSinkSubscribed}
						<button class="btn-primary" onclick={subscribeSqlSink}> 📡 Subscribe (Persistent) </button>
					{:else}
						<button class="btn-danger" onclick={unsubscribeSqlSink}> 🛑 Unsubscribe </button>
					{/if}

					<button
						class="btn-secondary"
						onclick={() => {
							sqlSinkUpdates = [];
						}}
					>
						🗑️ Clear Updates
					</button>

					<div class="status-badge" class:success={sqlSinkSubscribed}>
						{sqlSinkSubscribed ? '🟢 Connected' : '⚪ Not Connected'}
					</div>
				</div>

				{#if sqlSinkUpdates.length > 0}
					<div class="result-section">
						<h3>Real-Time Updates ({sqlSinkUpdates.length}):</h3>
						<div style="max-height: 400px; overflow-y: auto;">
							{#each sqlSinkUpdates as update}
								<div class="update-card">
									<div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
										<strong>
											{update.operation?.table || 'unknown'}.{update.operation?.operation ||
												'unknown'}
										</strong>
										<span style="color: #666; font-size: 0.9rem;">
											Value: {update.operation?.value || 0}
										</span>
									</div>
									<div style="font-size: 0.85rem; color: #888;">
										{new Date(Number(update.timestamp) * 1000).toLocaleString()}
									</div>
								</div>
							{/each}
						</div>
					</div>
				{:else if sqlSinkSubscribed}
					<div style="padding: 2rem; text-align: center; color: #666;">
						👂 Listening for updates... (waiting for ETL to process events)
					</div>
				{/if}
			</section>

			<!-- SQL Query Panel (HTTP) -->
			<section class="panel">
				<h2>🗄️ SQL Query via HTTP</h2>
				<p class="help">Query the SQL database via HTTP REST endpoint</p>

				<div class="form-group">
					<label>SQL Query:</label>
					<textarea
						bind:value={sqlQuery}
						placeholder="SELECT * FROM events LIMIT 10"
						rows="3"
						style="font-family: monospace; font-size: 0.9rem;"
					></textarea>
				</div>

				{#if sqlError}
					<div class="error" style="margin-bottom: 1rem;">⚠️ {sqlError}</div>
				{/if}

				<div class="button-group">
					<button
						class="btn-primary"
						onclick={executeSqlQuery}
						disabled={sqlLoading || !sqlQuery}
					>
						{sqlLoading ? 'Executing...' : 'Execute Query'}
					</button>
					<button class="btn-small" onclick={loadSqlEvents} disabled={sqlLoading}>
						Load All Events
					</button>
				</div>

				{#if sqlResults.length > 0}
					<div class="sql-results">
						<h3>Results ({sqlResults.length} rows):</h3>
						<div class="table-container">
							<table class="sql-table">
								<thead>
									<tr>
										{#each Object.keys(sqlResults[0]) as column}
											<th>{column}</th>
										{/each}
									</tr>
								</thead>
								<tbody>
									{#each sqlResults as row}
										<tr>
											{#each Object.values(row) as value}
												<td>{value}</td>
											{/each}
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{/if}
			</section>

			<!-- Log Sink Panel -->
			<section class="panel">
				<h2>📝 Log Sink gRPC Service</h2>
				<p class="help">
					Query and subscribe to logs via gRPC service (torii.sinks.log.LogSink)
				</p>

				{#if logError}
					<div class="error" style="margin-bottom: 1rem;">⚠️ {logError}</div>
				{/if}

				<div class="form-group">
					<label>Limit:</label>
					<input
						type="number"
						bind:value={logLimit}
						placeholder="10"
						min="1"
						max="100"
					/>
				</div>

				<div style="display: flex; gap: 0.5rem; margin-bottom: 1rem; flex-wrap: wrap;">
					<button
						class="btn-primary"
						onclick={queryLogs}
						disabled={logLoading}
					>
						{logLoading ? 'Loading...' : '📋 Query Logs'}
					</button>

					{#if !logSinkSubscribed}
						<button class="btn-secondary" onclick={subscribeLogSink}>
							📡 Subscribe to Updates
						</button>
					{:else}
						<button class="btn-danger" onclick={unsubscribeLogSink}>
							🛑 Unsubscribe
						</button>
					{/if}

					<button
						class="btn-secondary"
						onclick={() => {
							logResults = [];
							logSinkUpdates = [];
							logError = '';
						}}
					>
						🗑️ Clear
					</button>

					<div class="status-badge" class:success={logSinkSubscribed}>
						{logSinkSubscribed ? '🟢 Subscribed' : '⚪ Not Subscribed'}
					</div>
				</div>

				{#if logResults.length > 0}
					<div class="result-section">
						<h3>Recent Logs ({logResults.length}):</h3>
						<div class="table-container">
							<table>
								<thead>
									<tr>
										<th>ID</th>
										<th>Message</th>
										<th>Block</th>
										<th>Event Key</th>
										<th>Timestamp</th>
									</tr>
								</thead>
								<tbody>
									{#each logResults as log}
										<tr>
											<td>{log.id}</td>
											<td>{log.message}</td>
											<td>{log.block_number}</td>
											<td style="font-family: monospace; font-size: 0.8rem;">
												{log.event_key}
											</td>
											<td>{new Date(Number(log.timestamp) * 1000).toLocaleString()}</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{/if}

				{#if logSinkUpdates.length > 0}
					<div class="result-section">
						<h3>Real-Time Log Updates ({logSinkUpdates.length}):</h3>
						<div style="max-height: 400px; overflow-y: auto;">
							{#each logSinkUpdates as update}
								<div class="update-card">
									<div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
										<strong>Log #{update.log?.id || 'N/A'}</strong>
										<span style="color: #666; font-size: 0.9rem;">
											Block {update.log?.block_number || 'N/A'}
										</span>
									</div>
									<div style="margin-bottom: 0.5rem;">
										{update.log?.message || 'No message'}
									</div>
									<div style="font-size: 0.85rem; color: #888;">
										{new Date(Number(update.timestamp) * 1000).toLocaleString()}
									</div>
									{#if update.log?.event_key}
										<div style="font-size: 0.75rem; color: #999; font-family: monospace; margin-top: 0.25rem;">
											Event: {update.log.event_key}
										</div>
									{/if}
								</div>
							{/each}
						</div>
					</div>
				{:else if logSinkSubscribed}
					<div style="padding: 2rem; text-align: center; color: #666;">
						👂 Listening for log updates... (waiting for new events)
					</div>
				{/if}
			</section>
		</div>
	</div>
</main>

<style>
	:global(body) {
		margin: 0;
		padding: 0;
		font-family: system-ui, -apple-system, sans-serif;
		background: #f5f5f5;
	}

	.container {
		max-width: 1200px;
		margin: 0 auto;
		padding: 2rem;
	}

	header {
		text-align: center;
		margin-bottom: 2rem;
	}

	h1 {
		font-size: 2.5rem;
		margin: 0 0 0.5rem 0;
		color: #333;
	}

	.subtitle {
		color: #666;
		font-size: 1.1rem;
	}

	h2 {
		font-size: 1.3rem;
		margin: 0 0 1rem 0;
		color: #555;
	}

	.error {
		padding: 1rem;
		background: #f8d7da;
		color: #721c24;
		border-radius: 8px;
		margin-bottom: 1rem;
		border: 1px solid #f5c6cb;
	}

	.panels {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 1rem;
		margin-bottom: 1rem;
	}

	.panel {
		background: white;
		padding: 1.5rem;
		border-radius: 8px;
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
	}

	.status-panel {
		grid-column: 1 / -1;
		background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
		color: white;
		margin-bottom: 1rem;
	}

	.status-panel h2 {
		color: white;
	}

	.help {
		color: #666;
		font-size: 0.9rem;
		margin: 0 0 1rem 0;
	}

	.hint {
		color: #999;
		font-style: italic;
		margin-top: 0.5rem;
	}

	.updates-panel {
		grid-column: 1 / -1;
	}

	.panel-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 1rem;
	}

	.form-group {
		margin-bottom: 1rem;
	}

	label {
		display: block;
		margin-bottom: 0.5rem;
		color: #555;
		font-size: 0.9rem;
		font-weight: 500;
	}

	.required {
		color: #dc3545;
		font-weight: 600;
	}

	input[type='text'],
	input:not([type='checkbox']) {
		width: 100%;
		padding: 0.5rem;
		border: 1px solid #ddd;
		border-radius: 4px;
		font-size: 1rem;
		box-sizing: border-box;
		transition: border-color 0.2s;
	}

	input.input-error {
		border: 2px solid #dc3545;
		background: #fff5f5;
	}

	input:focus {
		outline: none;
		border-color: #0066cc;
	}

	input.input-error:focus {
		border-color: #dc3545;
	}

	input[type='checkbox'] {
		margin-right: 0.5rem;
	}

	input:disabled {
		background: #f5f5f5;
		cursor: not-allowed;
	}

	.button-group {
		display: flex;
		gap: 0.5rem;
		flex-wrap: wrap;
	}

	button {
		padding: 0.75rem 1.5rem;
		border: none;
		border-radius: 4px;
		font-size: 1rem;
		cursor: pointer;
		transition: all 0.2s;
		background: #e0e0e0;
		color: #333;
	}

	button:hover:not(:disabled) {
		background: #d0d0d0;
	}

	.btn-primary {
		background: #0066cc;
		color: white;
	}

	.btn-primary:hover {
		background: #0052a3;
	}

	.btn-success {
		background: #28a745;
		color: white;
	}

	.btn-success:hover {
		background: #218838;
	}

	.btn-danger {
		background: #dc3545;
		color: white;
	}

	.btn-danger:hover {
		background: #c82333;
	}

	.btn-small {
		padding: 0.5rem 1rem;
		font-size: 0.9rem;
	}

	.btn-tiny {
		padding: 0.25rem 0.5rem;
		font-size: 0.8rem;
		background: #6c757d;
		color: white;
		border: none;
		border-radius: 3px;
		cursor: pointer;
	}

	.btn-tiny:hover {
		background: #5a6268;
	}

	.status-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
		gap: 1rem;
		margin-bottom: 1rem;
	}

	.stat {
		text-align: center;
		padding: 1rem;
		background: rgba(255, 255, 255, 0.1);
		border-radius: 4px;
	}

	.stat-label {
		font-size: 0.85rem;
		margin-bottom: 0.5rem;
		opacity: 0.9;
	}

	.stat-value {
		font-size: 1.2rem;
		font-weight: 600;
	}

	.stat-value.client-id {
		font-size: 0.9rem;
		font-family: monospace;
		color: #ffd700;
	}

	.subscribed-topics {
		margin-top: 1.5rem;
		padding: 1rem;
		background: rgba(0, 255, 0, 0.05);
		border-radius: 4px;
		border: 1px solid rgba(0, 255, 0, 0.2);
	}

	.subscribed-topics h3 {
		font-size: 0.9rem;
		margin: 0 0 0.5rem 0;
		opacity: 0.9;
	}

	.subscribed-topics ul {
		list-style: none;
		padding: 0;
		margin: 0;
	}

	.subscribed-topics li {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 0.5rem;
		margin: 0.25rem 0;
		background: rgba(255, 255, 255, 0.05);
		border-radius: 3px;
	}

	.subscribed-topics li span {
		font-family: monospace;
		font-size: 0.9rem;
	}

	/* Multi-Topic Subscription Styles */
	.topic-form {
		padding: 1rem;
		background: rgba(255, 255, 255, 0.03);
		border-radius: 4px;
		margin-bottom: 1rem;
	}

	.topic-form h3 {
		font-size: 0.95rem;
		margin: 0 0 0.75rem 0;
	}

	.filters-section {
		margin: 1rem 0;
	}

	.filter-pair {
		display: flex;
		gap: 0.5rem;
		align-items: center;
		margin: 0.5rem 0;
	}

	.filter-pair span {
		font-weight: bold;
		color: rgba(255, 255, 255, 0.7);
	}

	.empty-topics-state {
		margin: 1.5rem 0;
		padding: 2rem;
		text-align: center;
		background: rgba(0, 0, 0, 0.02);
		border-radius: 4px;
		border: 2px dashed #ddd;
	}

	.active-topics {
		margin: 1rem 0;
		padding: 1rem;
		background: rgba(0, 255, 0, 0.05);
		border-radius: 4px;
		border: 1px solid rgba(0, 255, 0, 0.2);
	}

	.active-topics h3 {
		font-size: 0.95rem;
		margin: 0 0 0.75rem 0;
	}

	.topics-list {
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
	}

	.topic-item {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 0.75rem;
		background: rgba(255, 255, 255, 0.05);
		border-radius: 4px;
		border: 1px solid rgba(255, 255, 255, 0.1);
	}

	.topic-info {
		flex: 1;
	}

	.topic-info strong {
		display: block;
		margin-bottom: 0.25rem;
		color: #4fc3f7;
	}

	.topic-filters {
		display: flex;
		gap: 0.5rem;
		flex-wrap: wrap;
		margin-top: 0.5rem;
	}

	.filter-tag {
		padding: 0.25rem 0.5rem;
		background: rgba(100, 100, 255, 0.2);
		border-radius: 3px;
		font-size: 0.8rem;
		font-family: monospace;
		border: 1px solid rgba(100, 100, 255, 0.3);
	}

	.no-filters {
		font-size: 0.85rem;
		opacity: 0.6;
		font-style: italic;
	}

	.topic-actions {
		display: flex;
		gap: 0.5rem;
		align-items: center;
	}

	.unsub-checkbox {
		display: flex;
		align-items: center;
		gap: 0.25rem;
		font-size: 0.85rem;
		cursor: pointer;
	}

	.unsub-checkbox input[type='checkbox'] {
		cursor: pointer;
	}

	.unsub-notice {
		margin-top: 1rem;
		padding: 0.75rem;
		background: rgba(255, 165, 0, 0.1);
		border: 1px solid rgba(255, 165, 0, 0.3);
		border-radius: 4px;
		color: #ffa500;
		font-size: 0.9rem;
	}

	.btn-danger-tiny {
		background: #dc3545 !important;
	}

	.btn-danger-tiny:hover {
		background: #c82333 !important;
	}

	.btn-large {
		padding: 0.75rem 1.5rem;
		font-size: 1rem;
	}

	.hint {
		font-size: 0.85rem;
		opacity: 0.6;
		margin: 0.5rem 0;
		font-style: italic;
	}

	.stat-value.small {
		font-size: 0.9rem;
		font-family: monospace;
	}

	.stat-value.success {
		color: #d4edda;
	}

	.stat-value.inactive {
		color: rgba(255, 255, 255, 0.6);
	}

	/* SQL Results Styling */
	.sql-results {
		margin-top: 1.5rem;
		padding: 1rem;
		background: rgba(0, 100, 255, 0.05);
		border-radius: 4px;
		border: 1px solid rgba(0, 100, 255, 0.2);
	}

	.sql-results h3 {
		font-size: 0.9rem;
		margin: 0 0 1rem 0;
		opacity: 0.9;
	}

	.table-container {
		overflow-x: auto;
		max-height: 400px;
		overflow-y: auto;
		border: 1px solid rgba(255, 255, 255, 0.1);
		border-radius: 4px;
	}

	.sql-table {
		width: 100%;
		border-collapse: collapse;
		font-size: 0.85rem;
		font-family: monospace;
	}

	.sql-table thead {
		position: sticky;
		top: 0;
		background: rgba(0, 100, 255, 0.2);
		z-index: 10;
	}

	.sql-table th {
		padding: 0.75rem;
		text-align: left;
		font-weight: 600;
		border-bottom: 2px solid rgba(255, 255, 255, 0.2);
		white-space: nowrap;
	}

	.sql-table td {
		padding: 0.5rem 0.75rem;
		border-bottom: 1px solid rgba(255, 255, 255, 0.05);
		white-space: nowrap;
	}

	.sql-table tbody tr:hover {
		background: rgba(255, 255, 255, 0.05);
	}

	textarea {
		width: 100%;
		padding: 0.75rem;
		background: rgba(255, 255, 255, 0.05);
		border: 1px solid rgba(255, 255, 255, 0.2);
		border-radius: 4px;
		color: gray;
		resize: vertical;
		min-height: 80px;
	}

	textarea:focus {
		outline: none;
		border-color: #0066cc;
		background: rgba(255, 255, 255, 0.08);
	}

	.empty-state {
		text-align: center;
		padding: 3rem;
		color: #666;
	}

	.updates-list {
		max-height: 500px;
		overflow-y: auto;
	}

	.update-item {
		padding: 1rem;
		border: 1px solid #e0e0e0;
		border-radius: 4px;
		margin-bottom: 0.5rem;
		animation: slideIn 0.3s ease-out;
	}

	@keyframes slideIn {
		from {
			opacity: 0;
			transform: translateY(-10px);
		}
		to {
			opacity: 1;
			transform: translateY(0);
		}
	}

	.update-header {
		display: flex;
		gap: 1rem;
		align-items: center;
		margin-bottom: 0.5rem;
	}

	.update-type {
		padding: 0.25rem 0.75rem;
		border-radius: 12px;
		font-size: 0.75rem;
		font-weight: 600;
		text-transform: uppercase;
	}

	.update-type.type-0 {
		background: #d4edda;
		color: #155724;
	}

	.update-type.type-1 {
		background: #fff3cd;
		color: #856404;
	}

	.update-type.type-2 {
		background: #f8d7da;
		color: #721c24;
	}

	.update-topic {
		font-family: monospace;
		color: #0066cc;
		font-weight: 600;
	}

	.update-time {
		margin-left: auto;
		color: #999;
		font-size: 0.85rem;
	}

	.update-body {
		padding-left: 1rem;
	}

	/* Protobuf data display */
	.type-badge {
		padding: 0.25rem 0.5rem;
		background: #6c5ce7;
		color: white;
		border-radius: 4px;
		font-size: 0.7rem;
		font-family: monospace;
	}

	.protobuf-data {
		margin-top: 0.5rem;
	}

	.data-label {
		font-weight: 600;
		margin-bottom: 0.5rem;
		color: #6c5ce7;
	}

	.data-grid {
		display: grid;
		gap: 0.5rem;
	}

	.data-row {
		display: grid;
		grid-template-columns: 140px 1fr;
		gap: 1rem;
		padding: 0.5rem;
		background: rgba(108, 92, 231, 0.05);
		border-radius: 4px;
	}

	.data-key {
		font-weight: 600;
		color: #555;
	}

	.data-value {
		font-family: monospace;
		word-break: break-all;
	}

	.data-value.felt {
		color: #6c5ce7;
		font-size: 0.85rem;
	}

	.operation-badge {
		display: inline-block;
		padding: 0.25rem 0.75rem;
		border-radius: 12px;
		font-size: 0.85rem;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.operation-badge.insert {
		background: #d4edda;
		color: #155724;
		border: 1px solid #c3e6cb;
	}

	.operation-badge.update {
		background: #fff3cd;
		color: #856404;
		border: 1px solid #ffeaa7;
	}

	.operation-badge.delete {
		background: #f8d7da;
		color: #721c24;
		border: 1px solid #f5c6cb;
	}

	/* Topics Discovery Styles */
	.topics-discovery {
		margin-top: 1.5rem;
	}

	.topic-card {
		padding: 1.5rem;
		border: 1px solid rgba(255, 255, 255, 0.2);
		border-radius: 8px;
		margin-bottom: 1rem;
		background: rgba(255, 255, 255, 0.02);
		transition: all 0.3s ease;
	}

	.topic-card:hover {
		background: rgba(255, 255, 255, 0.05);
		border-color: #0066cc;
		transform: translateY(-2px);
	}

	.topic-header h4 {
		margin: 0;
		color: #0066cc;
		font-size: 1.25rem;
		font-weight: 600;
	}

	.topic-description {
		margin: 0.75rem 0;
		color: #ccc;
		line-height: 1.5;
	}

	.topic-filters-info {
		margin-top: 1rem;
		padding-top: 1rem;
		border-top: 1px solid rgba(255, 255, 255, 0.1);
	}

	.topic-filters-info strong {
		color: #fff;
		display: block;
		margin-bottom: 0.5rem;
	}

	.filter-tags {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem;
	}

	/* SQL Sink gRPC Service styles */
	.streaming-indicator {
		color: #ff6b6b;
		font-size: 0.9rem;
		margin-left: 0.5rem;
		animation: pulse 1.5s ease-in-out infinite;
	}

	@keyframes pulse {
		0%, 100% {
			opacity: 1;
		}
		50% {
			opacity: 0.5;
		}
	}

	.schema-table {
		background: #f8f9fa;
		border-radius: 6px;
		padding: 1rem;
		margin-bottom: 1rem;
	}

	.schema-table h4 {
		margin: 0 0 0.75rem 0;
		color: #0066cc;
		font-size: 1.1rem;
	}

	.schema-columns {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
		gap: 0.5rem;
	}

	.schema-column {
		background: white;
		padding: 0.5rem;
		border-radius: 4px;
		display: flex;
		justify-content: space-between;
		align-items: center;
		font-size: 0.9rem;
	}

	.col-name {
		font-weight: 600;
		color: #333;
	}

	.col-type {
		color: #666;
		font-family: 'Courier New', monospace;
		font-size: 0.85rem;
	}

	.status-badge {
		padding: 0.5rem 1rem;
		border-radius: 4px;
		font-size: 0.9rem;
		font-weight: 600;
		background: #e0e0e0;
		color: #666;
		display: inline-flex;
		align-items: center;
		gap: 0.5rem;
	}

	.status-badge.success {
		background: #d4edda;
		color: #155724;
	}

	.update-card {
		padding: 1rem;
		background: rgba(0, 100, 255, 0.05);
		border: 1px solid rgba(0, 100, 255, 0.2);
		border-radius: 4px;
		margin-bottom: 0.5rem;
	}

	@media (max-width: 968px) {
		.panels {
			grid-template-columns: 1fr;
		}

		.status-grid {
			grid-template-columns: repeat(2, 1fr);
		}
	}
</style>
