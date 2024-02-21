#!/usr/bin/env python3
import json
import random
import sys
import time

import numpy as np


def _load_coeffs(filename):
    with open(filename, encoding="utf-8") as f:
        return json.load(f)["COEFFS"]


def _evaluate(coeffs, x):
    return coeffs["a"] * x**2 + coeffs["b"] * x + coeffs["c"]


if __name__ == "__main__":
    #    if random.random() > 0.6:
    #        sys.exit(2)
    #
    #    if random.random() > 0.8:
    #        time.sleep(60)

    schedule = np.loadtxt("schedule.txt")

    coeffs = _load_coeffs("parameters_templated.json")
    output = [_evaluate(coeffs, x) for x in schedule]
    with open("poly.out", "w", encoding="utf-8") as f:
        f.write("\n".join(map(str, output)))
