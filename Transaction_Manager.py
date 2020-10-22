from typing import List
from enum import Enum, unique
import re
from Data_Manager import DataManager


class InvalidInstructionError(Exception):
    """Error thrown for invalid instructions"""

    def __init__(self, message):
        self.message = message


@unique
class OperationType(Enum):
    R = 0
    W = 1


class Operation:

    def __init__(self, command: OperationType, transaction_id: str, variable_id: str, value: int = None):
        """
        Initialize an operation, the operation type is only Read/Write
        :param command: R or W indicating the operation type
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to access
        :param value: the write value in write operation, by default is None
        """
        self.command = command
        self.transaction_id = transaction_id
        self.variable_id = variable_id
        self.value = value

    def __repr__(self):
        """
        Output operation object info
        """
        if self.value:
            return "{} ({}, {}, {})".format(self.command, self.transaction_id, self.variable_id, self.value)
        else:
            return "{} ({}, {})".format(self.command, self.transaction_id, self.variable_id)


class Transaction:

    def __init__(self, transaction_id: str, begin_ts: int, is_read_only: bool):
        """
        Initialize a Transaction
        :param transaction_id: id of this transaction
        :param begin_ts: begin timestamp of this transaction
        :param is_read_only: indicate whether this is a read-only transaction
        """
        self.transaction_id = transaction_id
        self.begin_ts = begin_ts
        self.is_read_only = is_read_only
        self.should_abort = False
        self.site_access_list = []

    def __repr__(self):
        """
        Output transaction object info
        """
        if self.is_read_only:
            return "[{}, begin at {}, read-only]".format(self.transaction_id, self.begin_ts)
        else:
            return "[{}, begin at {}]".format(self.transaction_id, self.begin_ts)


# Transaction Manager is used to process all instructions and conduct corresponding operations for various transactions
class TransactionManager:


    def __init__(self):
        """
        Initialize Transaction Manager
        Call DataManager to finish initialization of all sites
        """
        self.ts = 0 # record current timestamp
        self.transaction_table = {} # transaction table to record all transactions
        self.operation_set = set() # all operations which wait to be executed, Read/Write
        self.site_list = [DataManager(site_id) for site_id in range(1,11)] # list of all sites


    def dump(self):
        """
        Output all useful info about all sites
        Call DM to print info about each site
        """
        for site in self.site_list:
            site.dump()


    # process a line from input file
    def process_line(self, line: str):
        """
        Parse a line and pass all paras to process_command
        :param line: a line from input file
        """
        paras = re.findall(r"[\w']+", line)
        # print(paras)
        command = paras.pop(0)
        print("processing instruction {}({})".format(command,paras))
        self.process_command(command, paras)
        self.execute_operations()
        self.ts += 1 # a newline in the input means time advances by one


    def process_command(self, command: str, paras: List[str]):
        """
        Do corresponding operation according to the command and paras
        :param command: ["begin", "beginRO", "read", "write", "dump", "end", "fail", "recover"]
        :param paras: a list of paras like [T1,x1,101]
        """
        if command == "begin":
            self.beigin(paras[0],False)
        elif command == "beginRO":
            self.beigin(paras[0],True)
        elif command == "R":
            self.add_read_opration(paras[0],paras[1])
        elif command == "W":
            self.add_write_opration(paras[0],paras[1],int(paras[2]))
        elif command == "dump":
            # self.dump()
            pass
        elif command == "end":
            self.end(paras[0])
        elif command == "fail":
            self.fail(paras[0])
        elif command == "recover":
            self.recover(paras[0])
        else:
            raise InvalidInstructionError("Unknown Instruction: " + command)


    def execute_operations(self):
        """
        Loop through operation set, call read/write to execute all
        If an execution succeed, remove it from set, otherwise let it remain there
        :return:
        """
        if self.operation_set: print("==================================")
        success = None
        for operation in list(self.operation_set): # operation_set can not be changed during iteration
            cur_transaction: Transaction = self.transaction_table.get(operation.transaction_id)
            # first judge whether the transaction containing this op still exist
            if not cur_transaction:
                self.operation_set.remove(operation)
                continue
            if operation.command == OperationType.R:
                if cur_transaction.is_read_only:
                    success = self.read_snapshot(operation.transaction_id,operation.variable_id)
                else:
                    success = self.read(operation.transaction_id,operation.variable_id)
            elif operation.command == OperationType.W:
                success = self.write(operation.transaction_id,operation.variable_id,operation.value)
            else:
                print("Invalid command {} in operation".format(operation.command))

            # Read/Write succeed, remove this operation from operation set
            if success:
                self.operation_set.remove(operation)

            # Output execution info
            status = "success" if success else "fail"
            print("Executed operation: {} [{}], Remaining operations: {}".format(operation, status, self.operation_set))

        if success!=None: print("")


    def add_read_opration(self, transaction_id: str, variable_id: str):
        """
        Add read operation to operation queue for future execution
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to access
        """
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError("{} doesn't exist".format(transaction_id))
        self.operation_set.add(Operation(OperationType.R, transaction_id, variable_id))


    def read(self, transaction_id: str, variable_id: str):
        """
         A transaction T want to read a variable i
         Call DM to read from any sites which have this variable
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to access
        :return: True means read succeed, False means read fail
        """
        cur_transaction: Transaction = self.transaction_table.get(transaction_id)
        if not cur_transaction:
            raise InvalidInstructionError("{} doesn't exist".format(transaction_id))

        for site in self.site_list:
            if site.is_up and site.has_variable(variable_id):
                return_result = site.read(transaction_id,variable_id)
                # read is success, update transaction's site_access list
                if return_result.success:
                    cur_transaction.site_access_list.append(site.site_id)
                    print("{} successfully read {} from site {}, return {}".
                          format(transaction_id, variable_id, site.site_id,return_result.value))
                    return True
        return False


    # a read-only transaction T want to read a snapshot of variable i
    def read_snapshot(self, transaction_id, variable_id):
        # call DM to read from any sites which have this variable,
        # return True or False, which indicate whether this read is success or fail
        return True


    def add_write_opration(self, transaction_id: str, variable_id: str, value: int):
        """
        Add write operation to operation queue for future execution
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to access
        :param value: value which T wants to write to variable
        """
        if not self.transaction_table.get(transaction_id):
            raise InvalidInstructionError("{} doesn't exist".format(transaction_id))
        self.operation_set.add(Operation(OperationType.W, transaction_id, variable_id, value))


    # a transaction T want to write a variable i to value X
    def write(self, transaction_id, variable_id, value):
        # call DM to write to all up sites, as long as write lock can be acquired,
        # return True or False, which indicate whether this write is success or fail
        return False


    def beigin(self, transaction_id: str, is_read_only: bool):
        """
        Begin a transaction
        :param transaction_id: id of this transaction
        :param is_read_only: True/False indicating whether this is a read-only transaction
        """
        # Initialize this transaction with current timestamp and add it into transaction table.
        if self.transaction_table.get(transaction_id):
            raise InvalidInstructionError("{} already begins".format(transaction_id))
        self.transaction_table[transaction_id] = Transaction(transaction_id, self.ts, is_read_only)

        # print transaction begin info
        if is_read_only:
            print("read-only transaction {} begins".format(transaction_id))
        else:
            print("transaction {} begins".format(transaction_id))


    def end(self, transaction_id: str):
        """
        End a transaction, if the abort flag of this transaction is true, abort it. Otherwise commit it.
        :param transaction_id: id of this transaction
        """
        cur_transaction: Transaction = self.transaction_table.get(transaction_id)
        if not cur_transaction:
            raise InvalidInstructionError("{} doesn't exist".format(transaction_id))
        if cur_transaction.should_abort:
            self.abort(cur_transaction.transaction_id)
        else:
            self.commit(cur_transaction.transaction_id)


    # a transaction is about to abort, do corresponding operations
    def abort(self, transaction_id):
        # call DM to abort this transaction, remove transaction id from transaction table in TM.
        print("transaction {} abort".format(transaction_id))


    # a transaction is about to commit, do corresponding operations
    def commit(self, transaction_id):
        # call DM to commit this transaction, remove transaction id from transaction table in TM.
        print("transaction {} commit".format(transaction_id))


    # a site fail, do corresponding operations for related transactions
    def fail(self, site_id):
        # call DM to do failure operations in site
        # for all transactions which have ever accessed this failed site, set their abort flag to true
        pass


    # a site recover, do corresponding operations for related transactions
    def recover(self, site_id):
        # call DM to do recovery operations in site
        pass


    # return this site’s wait for graph for cycle detection
    def solve_deadlock(self):
        # collect waits-for graphs from all sites, then abort youngest transaction if there is a cycle.
        pass
