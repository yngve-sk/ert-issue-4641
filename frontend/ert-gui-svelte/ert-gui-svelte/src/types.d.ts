export interface Experiment {
    id: string,
    type: string
}

type Status = "Waiting" | "Pending"

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

