<script lang="ts">
  interface Props {
    connected: boolean;
    clientId: string;
    serverUrl: string;
    updateCount: number;
    onSubscribe: () => void;
    onDisconnect: () => void;
  }

  let { connected, clientId, serverUrl, updateCount, onSubscribe, onDisconnect }: Props = $props();
</script>

<section class="panel status-panel full-width">
  <h2>Status</h2>
  <div class="status-grid">
    <div class="stat">
      <div class="stat-label">Connection</div>
      <div class="stat-value" class:success={connected}>
        {connected ? "Connected" : "Disconnected"}
      </div>
    </div>
    <div class="stat">
      <div class="stat-label">Server</div>
      <div class="stat-value">{serverUrl}</div>
    </div>
    <div class="stat">
      <div class="stat-label">Client ID</div>
      <div class="stat-value mono">{clientId}</div>
    </div>
    <div class="stat">
      <div class="stat-label">Updates</div>
      <div class="stat-value">{updateCount}</div>
    </div>
  </div>
  <div class="btn-group" style="margin-top: 1rem; justify-content: center;">
    {#if connected}
      <button class="btn btn-danger" onclick={onDisconnect}>Disconnect</button>
    {:else}
      <button class="btn btn-primary" onclick={onSubscribe}>Subscribe</button>
    {/if}
  </div>
</section>
