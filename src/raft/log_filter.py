#!/bin/python3
import sys
'''
    filter log
    args: log_input_file, watch peer1, watch peer2
'''

def main():
    _, log_file, a, b = sys.argv
    x_label = {
        f"{a} -> {b}",
        f"{b} -> {a}",
        f"{a} <- {b}",
        f"{b} <- {a}",
        f"{a}",
        f"{b}",
    }
    print(f"x_label: {x_label}")
    log_file_output = log_file[:-4] + "_output.log"
    with open(log_file) as input:
        with open(log_file_output, 'w') as output:
            for index, line in enumerate(input):
                s = line.split(': ', maxsplit=2)
                if len(s) < 3:
                    continue
                _, x, _ = s
                if x in x_label:
                    output.write(f"{index+1}\t{line}")


if __name__ == '__main__':
    main()