<script lang="ts">
    import { experiments, selectedExperimentId } from "../stores/store"
</script>

<div>
    <div>Selected experiment: {$selectedExperimentId}</div>
    <p>All experiments</p>
    <div class="exps-container">
        {#each $experiments as exp}
            <div
                role="button"
                class="exp-wrap"
                tabindex="0"
                class:selected={exp.id === $selectedExperimentId}
                on:click={() => selectedExperimentId.set(exp.id)}
                on:keypress={() => {}}
                >
                <div class="exp-title">{exp.type} <br/><i>{exp.id}</i></div>
            </div>
        {/each}
    </div>
</div>

<style>
    .exp-wrap {
        display: flex;
        max-width: 250px;
        margin: 5px;
        border-radius: 5px;
        color: gray;
        justify-content: center;
        align-items: center;
        background: #0000004d;
        border: thin #ffffff3d solid;
        user-select: none;
        transition: background-color 400ms cubic-bezier(0.25, 0.1, 0.25, 1);
    }

    .exp-wrap.selected, .exp-wrap:hover {
        background: #3535354d;
        border: 1px #ffffff3d solid;
        color: white;
    }

    .exps-container {
        background: #0000004f;
        display: flex;
    }

</style>
