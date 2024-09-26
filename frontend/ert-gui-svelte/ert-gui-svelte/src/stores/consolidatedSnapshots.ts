import { writable, type Writable, get } from 'svelte/store'
import type { Experiment, FullSnapshotEvent, SnapshotUpdateEvent } from '../types'
import { merge } from "lodash"

export const renderedSnapshots: Writable<any> = writable([])

const _consolidatedSnapshots: (SnapshotUpdateEvent | FullSnapshotEvent)[] = []
export const writeSnapshot = (event: SnapshotUpdateEvent | FullSnapshotEvent) => {
    // Find iteration
    const iter = event.iteration
    if (iter === _consolidatedSnapshots.length) {
        _consolidatedSnapshots.push(event)
    } else {
        merge(_consolidatedSnapshots[iter], event)
    }

    renderedSnapshots.set(_consolidatedSnapshots)
}
