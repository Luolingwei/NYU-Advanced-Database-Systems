import os
import sys
from Transaction_Manager import TransactionManager


if __name__ == '__main__':

    input_path = sys.argv[1] if len(sys.argv) >= 2 else None

    # input_path para is NOT empty, scan all test cases under this path
    if input_path!=None:
        if input_path=="":
            print("No input path detected, using default test cases defined by us")
            input_path = "./test_cases"
        for file in os.listdir(input_path):
            if not file.startswith("test"): continue
            cur_file = input_path + '/' + file
            f = open(cur_file, "r")
            print("\n")
            print("########################################################################")
            print("################ Reading from " + cur_file + "... " + "###################")
            print("########################################################################")

            # begin to process commands in 1 input file
            ts_manager = TransactionManager()
            for line in f.readlines():
                line = line.strip()
                if line and not line.startswith(("#","//")):
                    ts_manager.process_line(line)

    # input_path para is empty, accept test case from standard input
    else:
        ts_manager = TransactionManager()
        print("Reading input from standard input... (please enter \"exit\" to exit program)")
        while True:
            line = input()
            if line.strip() == "exit":
                break
            ts_manager.process_line(line)
            print("####################################################")