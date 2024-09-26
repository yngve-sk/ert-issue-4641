
<script lang="ts">
import { renderedSnapshots } from "../stores/consolidatedSnapshots"
</script>

<div class="rundialog-container">
    {#each $renderedSnapshots as sn}
    <div class="iteration-container">
        {#each Object.entries(sn.snapshot.reals) as [real, rstate]}
        <div>iter-{sn.iteration}, realization{real} - {rstate.status}</div>
        {/each}
    </div>
    {/each}
</div>