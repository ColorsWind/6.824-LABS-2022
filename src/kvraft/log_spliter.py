#!/bin/python3
from __future__ import annotations

import sys

def main():
    log_file = sys.argv[1]
    with open(log_file, 'r') as f:
        log_lines = f.readlines()
    if len(sys.argv) == 2:
        for line in log_lines:
            if line.startswith('Test: '):
                print(f"'{line.strip()}'")
    else:
        tag = sys.argv[2]
        begin = -1
        end = len(log_lines)
        for index, line in enumerate(log_lines):
            if tag in line:
                begin = index
            elif line.startswith('Test: ') and begin >= 0:
                end = index
                break
        log_file_output = log_file[:-4] + "_output.log"
        print(f"Write line: {begin+1} ~ {end} {log_file_output}")

        with open(log_file_output, 'w') as f:
            for line in log_lines[begin:end]:
                if not line.startswith('Raft'):
                    f.write(line)

if __name__ == '__main__':
    main()