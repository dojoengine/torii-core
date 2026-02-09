<script lang="ts">
  import { formatTimestamp, getUpdateTypeName } from "@torii-tokens/shared";

  interface Update {
    topic: string;
    updateType: number;
    timestamp: number;
    typeId: string;
    data?: unknown;
  }

  interface Props {
    updates: Update[];
    connected: boolean;
    onClear: () => void;
  }

  let { updates, connected, onClear }: Props = $props();
</script>

<section class="panel full-width">
  <div class="panel-header">
    <h2>Real-time Updates ({updates.length})</h2>
    <button class="btn btn-sm" onclick={onClear}>Clear</button>
  </div>

  {#if updates.length === 0}
    <div class="empty-state">
      {connected ? "Waiting for updates..." : "Subscribe to start receiving updates"}
    </div>
  {:else}
    <div class="updates-list">
      {#each updates as u, i (i)}
        <div class="update-item">
          <div class="update-header">
            <span class="badge badge-{getUpdateTypeName(u.updateType).toLowerCase()}">
              {getUpdateTypeName(u.updateType)}
            </span>
            <span class="update-topic">{u.topic}</span>
            <span class="update-time">{formatTimestamp(u.timestamp)}</span>
          </div>
          <div class="update-body">
            <pre style="font-size: 0.85rem; overflow-x: auto;">{JSON.stringify(u.data, null, 2)}</pre>
          </div>
        </div>
      {/each}
    </div>
  {/if}
</section>
