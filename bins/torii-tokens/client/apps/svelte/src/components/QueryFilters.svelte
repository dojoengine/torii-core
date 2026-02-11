<script lang="ts">
  interface Props {
    onQuery: (contractAddress: string, wallet: string) => void;
    loading?: boolean;
  }

  let { onQuery, loading = false }: Props = $props();

  let contractAddress = $state("");
  let wallet = $state("");

  function handleSubmit(e: Event) {
    e.preventDefault();
    if (contractAddress || wallet) {
      onQuery(contractAddress, wallet);
    }
  }
</script>

<section class="panel query-panel full-width">
  <h2>Query Balances & Transfers</h2>
  <form onsubmit={handleSubmit}>
    <div class="form-grid">
      <div class="form-group">
        <label for="contractAddress">Contract Address</label>
        <input
          type="text"
          id="contractAddress"
          bind:value={contractAddress}
          placeholder="0x..."
          class="input"
        />
      </div>
      <div class="form-group">
        <label for="wallet">Wallet Address</label>
        <input
          type="text"
          id="wallet"
          bind:value={wallet}
          placeholder="0x..."
          class="input"
        />
      </div>
    </div>
    <div class="btn-group" style="margin-top: 1rem; justify-content: center;">
      <button
        type="submit"
        class="btn btn-primary"
        disabled={(!contractAddress && !wallet) || loading}
      >
        {loading ? "Loading..." : "Query"}
      </button>
    </div>
  </form>
</section>
