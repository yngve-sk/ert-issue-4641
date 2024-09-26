import { readable, writable, type Readable, type Writable, get } from 'svelte/store'

const urlParams = new URLSearchParams(window.location.search)
const serverURL = decodeURIComponent(
    urlParams.get('serverURL') || 'http://localhost:8001',
)

// Just for dev
const lag = async (t: number): Promise<void> =>
    new Promise((resolve) => setTimeout(resolve, t))

const minLagMs = 0

const createQueryString = (obj: object): string => {
    const urlParams = new URLSearchParams()
    Object.entries(obj).forEach(([k, v]) => urlParams.append(k, v.toString()))
    return urlParams.toString()
}


export interface Experiment {
    id: string,
    type: string
}

export const experiments: Writable<Experiment[]> = writable([])
export const selectedExperimentId: Writable<string> = writable("")

const fetchExperiments = async () => {
    try {
      const res = await fetch('http://127.0.0.1:8000/experiments/');
      if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
      }
      const data = await res.json();
      experiments.set(data);
      
      if (data.length > 0 && !get(selectedExperimentId)) {
        selectedExperimentId.set(data[0].id);
      }
    } catch (error) {
      console.error("Failed to fetch experiments:", error);
    }
}

setInterval(fetchExperiments, 1000)

type Status = "Waiting" | "Pending"

class A {}
class B {}

export interface RealizationState {
    status: Status
    active: boolean
    forward_models: Record<string, {
        status: string,
        index: string,
        name: string,
    }>
}

export interface FullSnapshotEvent {
    event_type: "FullSnapshotEvent",
    iteration_label: string, 
	current_iteration: number,
	total_iterations: number,
	progress: number,
	realization_count: number,
	status_count: {
        Pending?: number
		Waiting?: number
	},
	iteration: 0,
	timestamp: string,
	snapshot: {
        metadata: {
            aggr_job_status_colors: Record<string,string>,
            real_status_colors: Record<string, string>,
            sorted_real_ids: string[],
            sorted_forward_model_ids: Record<string, string>,
        },
        status: "Unknown",
        reals: Record<string, RealizationState>
    }
}

let hasRunWS = false
const allEvents: (FullSnapshotEvent | any)[] = []
export const renderedEvents: Writable<(FullSnapshotEvent | any)[]> = writable([])
const ws = () => {
    if (!get(selectedExperimentId)) return;

    if (hasRunWS) return
    hasRunWS = true
    const wsUrl = `ws://127.0.0.1:8000/experiments/${get(selectedExperimentId)}/events`;
    const socket = new WebSocket(wsUrl);

    socket.onmessage = function (event) {
      console.log('Message from WS Server', event.data);
      const data = JSON.parse(event.data)
      allEvents.push(data)
    };

    socket.onopen = () => console.log('Connected to WS Server', wsUrl);
    socket.onclose = () => console.log('Disconnected from WS Server');
    socket.onerror = (error) => console.error('WebSocket Error:', error);

    return () => {
      socket.close();
    };
  }

  ws()

 
  setInterval(ws, 1000)

  let eventIndex = 0
  let theInterval = null
  const graduallyAddRenderedEvents = () => {
    renderedEvents.set(allEvents.slice(0, ++eventIndex))
  }
  
  theInterval = setInterval(graduallyAddRenderedEvents, 200)

  selectedExperimentId.subscribe((value) => {
    hasRunWS = false
    allEvents.length = 0
    eventIndex = 0
  })
