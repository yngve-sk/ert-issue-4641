<script lang="ts">
    import { renderedEvents, experiments, type Experiment } from "../stores/store"
    import ExperimentsView from "./ExperimentsView.svelte";
</script>

<div>
    <p>All experiments</p>
    {#each $experiments as exp}
        <div>{exp.type}</div>
    {/each}
</div>