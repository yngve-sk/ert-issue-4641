#!/usr/bin/env python3

if __name__ == "main":
    with open("poly_schedule.txt", "w+") as f:
        f.write(",".join(map(str, [*range(10)])))
