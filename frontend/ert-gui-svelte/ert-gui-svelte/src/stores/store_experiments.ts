import { writable } from 'svelte/store';

// Create writable stores
export const experiments = writable([]);
export const selectedExperimentId = writable('');
export const progress = writable(0);

// Fetch experiments and update the store
const fetchExperiments = async () => {
    try {
        const res = await fetch('/api/experiments');
        if (!res.ok) {
            throw new Error(`HTTP error! status: ${res.status}`);
        }
        const data = await res.json();
        experiments.set(data);
        
        selectedExperimentId.update((id) => {
            // If no experiment is selected, pick the first one
            if (!id && data.length > 0) {
                return data[0].id;
            }
            return id;
        });
    } catch (error) {
        console.error('Failed to fetch experiments:', error);
    }
};

// Fetch experiments initially and set up polling every 5 seconds
fetchExperiments();
const intervalId = setInterval(fetchExperiments, 5000);

// Optional: clear the interval when needed, like on component destroy
export const stopPolling = () => clearInterval(intervalId);
