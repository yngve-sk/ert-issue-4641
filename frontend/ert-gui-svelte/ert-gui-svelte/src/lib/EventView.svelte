<script lang="ts">
    import { renderedEvents, experiments } from "../stores/store"
     function getColor(e: event) {
        switch (e.event_type) {
            case 'SnapshotUpdateEvent':
                return "Navy"
            case "FullSnapshotEvent":
                return "Maroon"
            case "EndEvent":
                return "Olive"
            default:
                return "SandyBrown"
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
        <div class="single-event" style="background-color: {getColor(e)}" in:fade={{ duration: 2000 }}>{e.event_type.split("Event")[0]}</div>
    {/each}
    </div>
</div>

<style>
 .events-container {
    display: flex;
    flex-flow: wrap;
    justify-content: flex-start;
    width: 100%;
 }
 .single-event {
    margin: 10px;
    color: white;
    width: 200px;
    height: 70px;
    align-items: center;
    justify-content: center;
    display: flex;
 }
</style>
