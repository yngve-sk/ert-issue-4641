<script lang="ts">
    import { renderedEvents, experiments } from "../stores/store"
     function getColor(e: event) {
        switch (e.event_type) {
            case 'SnapshotUpdateEvent':
                return "blue"
            case "FullSnapshotEvent":
                return "red"
            case "EndEvent":
                return "green"
        return "pink"
     }
    }

    import { fade } from 'svelte/transition';
  let visible = false;

  // Function to toggle visibility
  function toggleVisibility() {
    visible = !visible;
  }
</script>


<div>
  <p>All Events</p>
    <div class="events-container">
    {#each $renderedEvents as e}
        <div class="single-event" style="background-color: {getColor(e)}" in:fade={{ duration: 2000 }}>{e.event_type}</div>
    {/each}
    </div>
</div>

<style>
 .events-container {
    display: flex;
    flex-flow: wrap;
    justify-content: flex-start;
    width: 100vw;
 }
 .single-event {
    margin: 10px;
    color: white;
    padding: 20px 5px 20px 5px;
 }
</style>
