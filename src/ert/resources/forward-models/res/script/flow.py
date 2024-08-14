#!/usr/bin/env python
import sys

from flow_config import FlowConfig
from flow_run import run

if __name__ == "__main__":
    config = FlowConfig()
    run(config, [arg for arg in sys.argv[1:] if len(arg) > 0])
