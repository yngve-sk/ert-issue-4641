<script lang="ts">
    import { renderedEvents, experiments, type Experiment } from "../stores/store"
</script>


<div>
  <p>All Events</p>
    {#each $renderedEvents as e}
        <div>{e.event_type}</div>
    {/each}
</div>