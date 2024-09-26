#!/bin/bash
fastapi run src/ert/experiment_server/main.py &
pushd frontend/ert-gui-svelte/ert-gui-svelte/
npm i -D vite
npm i
node_modules/.bin/vite
