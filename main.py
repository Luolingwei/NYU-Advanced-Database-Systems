import os
from Transaction_Manager import TransactionManager

if __name__ == '__main__':

    # input_path = input("Please enter path of your test case folder \n")
    input_path = "test_cases"
    for file in os.listdir(input_path):
        if not file.startswith("test"): continue
        cur_file = input_path + '/' + file
        f = open(cur_file, "r")
        print("\n")
        print("Reading from " + cur_file)

        ts_manager = TransactionManager()
        for line in f.readlines():
            line = line.strip()
            if line and not line.startswith(("#","//")):
                ts_manager.process_line(line)