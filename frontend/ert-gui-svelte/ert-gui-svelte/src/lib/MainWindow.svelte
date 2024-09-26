<script lang="ts">
    import { renderedEvents, experiments, type Experiment } from "../stores/store"
  import EventView from "./EventView.svelte";
  import ExperimentsView from "./ExperimentsView.svelte";
  import RunDialog from "./RunDialog.svelte";
</script>

<div>
    <ExperimentsView/>
    <EventView/>
    <RunDialog/>
</div>
