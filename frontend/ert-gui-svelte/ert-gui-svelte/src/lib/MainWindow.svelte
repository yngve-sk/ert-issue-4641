<script lang="ts">
    import { renderedEvents, experiments, type Experiment } from "../stores/store"
</script>

<div>
    <p>All experiments</p>
    {#each $experiments as exp}
        <div>{exp.type}</div>
    {/each}
    <p>All Events</p>
    {#each $renderedEvents as e}
        <div>{e.event_type}</div>
    {/each}
</div>
