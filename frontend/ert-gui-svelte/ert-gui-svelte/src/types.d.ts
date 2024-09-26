export interface Experiment {
    id: string,
    type: string
}

type Status = "Waiting" | "Pending"

interface ForwardModelState {
    "status": Status,
    "index": number,
    "start_time": string,
    "stdout": string,
    "stderr": string,
    "current_memory_usage": number,
    "max_memory_usage": number,
    "end_time": string,
    "error": string
}

export interface RealizationState {
    status: Status
    active: boolean
    start_time: string,
    forward_models: Record<string, ForwardModelState>
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


export interface SnapshotUpdateEvent {
    event_type: "SnapshotUpdateEvent",
    iteration_label: string,
	current_iteration: number,
	total_iterations: number,
	progress: number,
	realization_count: number,
	status_count: {
        Pending?: number
		Waiting?: number
        Running?: number
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
        status: string
        reals?: Record<string, RealizationState>
    }
}