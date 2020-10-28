import re
from collections import defaultdict
from typing import List
from Utils import InvalidCommandError, OperationType, Operation, Transaction
from Data_Manager import DataManager


# Transaction Manager is used to process all instructions and conduct corresponding operations for various transactions
class TransactionManager:

    def __init__(self):
        """
        Initialize Transaction Manager
        Call DataManager to finish initialization of all sites
        """
        self.ts = 0 # record current timestamp
        self.transaction_table = {} # transaction table to record all transactions, {transaction_id: Transaction}
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
        # if there is deadlock detected, we will execute operation queue once more, as deadlock has been cleaned, may succeed now
        if self.solve_deadlock():
            self.execute_operations()
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
            self.fail(int(paras[0]))
        elif command == "recover":
            self.recover(int(paras[0]))
        else:
            raise InvalidCommandError("Unknown Instruction: " + command)


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
            raise InvalidCommandError("{} doesn't exist".format(transaction_id))
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
            raise InvalidCommandError("{} doesn't exist".format(transaction_id))

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
        """
        A transaction T want to read a variable i
        Call DM to read from any sites which have this variable
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to access
        :return: True means read succeed, False means read fail
        """
        if not self.transaction_table.get(transaction_id):
            raise InvalidCommandError("{} does not exist".format(transaction_id))

        begin_ts = self.transaction_table[transaction_id].begin_ts
        for site in self.site_list:
            if site.is_up and site.has_variable(variable_id):
                result = site.read_snapshot(variable_id, begin_ts)
                if result.success:
                    print("{} (RO) reads {}.{}: {}".format(transaction_id, variable_id, site.site_id, result.value))
                    return True
        return False


    def add_write_opration(self, transaction_id: str, variable_id: str, value: int):
        """
        Add write operation to operation queue for future execution
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to access
        :param value: value which T wants to write to variable
        """
        if not self.transaction_table.get(transaction_id):
            raise InvalidCommandError("{} doesn't exist".format(transaction_id))
        self.operation_set.add(Operation(OperationType.W, transaction_id, variable_id, value))


    def write(self, transaction_id: str, variable_id: str, value: int):
        """
        A transaction T want to write value X to a variable i
        Call DM to write to all up sites, as long as write lock can be acquired
        Need to first check whether can obtain all write locks, if not, can not write
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to write
        :param value: value which T wants to write to var
        :return: Return True or False, which indicate whether this write is success or fail
        """
        cur_transaction: Transaction = self.transaction_table.get(transaction_id)
        if not cur_transaction:
            raise InvalidCommandError("{} doesn't exist".format(transaction_id))

        # judge whether all relevant up sites can be written
        can_get_all_write_lock = True
        all_relevant_site_down = True
        for site in self.site_list:
            if site.is_up and site.has_variable(variable_id):
                all_relevant_site_down = False
                if not site.can_get_write_lock(transaction_id, variable_id):
                    can_get_all_write_lock = False

        # all relevant up sites can be written
        if not all_relevant_site_down and can_get_all_write_lock:
            for site in self.site_list:
                if site.is_up and site.has_variable(variable_id):
                    site.write(transaction_id, variable_id, value)
                    cur_transaction.site_access_list.append(site.site_id)
                    print("{} successfully write {} to {} in site {}".
                          format(transaction_id, variable_id, value, site.site_id))
            return True

        # at least 1 relevant up site can not be written, give up
        else:
            return False


    def beigin(self, transaction_id: str, is_read_only: bool):
        """
        Begin a transaction
        :param transaction_id: id of this transaction
        :param is_read_only: True/False indicating whether this is a read-only transaction
        """
        # Initialize this transaction with current timestamp and add it into transaction table.
        if self.transaction_table.get(transaction_id):
            raise InvalidCommandError("{} already begins".format(transaction_id))
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
            raise InvalidCommandError("{} doesn't exist".format(transaction_id))
        if cur_transaction.should_abort:
            self.abort(cur_transaction.transaction_id)
        else:
            self.commit(cur_transaction.transaction_id, self.ts)



    def abort(self, transaction_id):
        """
        call DM to abort this transaction
        update the transaction table in TM.
        :param transaction_id: id of this transaction
        """
        for site in self.site_list:
            site.abort(transaction_id)
        self.transaction_table.pop(transaction_id)
        print("{} abort".format(transaction_id))



    def commit(self, transaction_id, commit_ts):
        '''
        call DM to commit this transaction
        update the transaction table in TM.
        :param transaction_id: id of this transaction
        :param commit_ts: timestamp when commit
        '''
        for site in self.site_list:
            site.commit(transaction_id, commit_ts)
        self.transaction_table.pop(transaction_id)
        print("{} commits!".format(transaction_id))



    def fail(self, site_id: int):
        """
        A site fail, all transactions which ever access this site should abort eventually
        :param site_id: id of the site to fail
        """
        if not 1 <= site_id <= len(self.site_list):
            raise InvalidCommandError("try to fail site with id {}, which doesn't exist".format(site_id))

        site = self.site_list[site_id-1]
        site.fail(self.ts)
        print("site {} fail at time {}".format(site_id, self.ts))
        for transaction in self.transaction_table.values():
            # skip read-only T and already to be aborted T
            if transaction.is_read_only or transaction.should_abort:
                continue
            if site_id in transaction.site_access_list:
                transaction.should_abort = True
                print("Set transaction {}'s should_abort flag to True".format(transaction.transaction_id))


    def recover(self, site_id: int):
        """
        A site fail, all transactions which ever access this site should abort eventually
        :param site_id: id of the site to recover
        :return:
        """
        site_id = int(site_id)
        if not 1 <= site_id <= len(self.site_list):
            raise InvalidCommandError("try to recover site with id {}, which doesn't exist".format(site_id))

        site = self.site_list[site_id-1]
        site.recover(self.ts)
        print("site {} recover at time {}".format(site_id, self.ts))


    def solve_deadlock(self):
        """
        Detect global deadlock and solve by aborting youngest transaction
        :return: True means deadlock detected, a transaction is aborted. False means no deadlock and no action happened.
        """

        # check whether we can start from node and return to same node
        def dfs(node, target):
            if len(visited)!= 0 and node == target: return True
            for nei in global_graph[node]:
                if nei not in visited:
                    visited.add(nei)
                    if dfs(nei, target): return True
            return False

        # collect waits-for graphs from all sites
        global_graph = defaultdict(set)
        for site in self.site_list:
            if site.is_up:
                cur_graph = site.get_wait_for_graph()
                for node, wait_set in cur_graph.items():
                    global_graph[node] |= wait_set

        if len(global_graph.keys())>0: print("current global wait-for graph is {}".format(global_graph))
        # detect possible cycle in global graph
        youngest_transaction = None
        for start_node in list(global_graph.keys()):
            visited = set()
            # if we can start from this node and return to this node, this node is a member of cycle
            if dfs(start_node, start_node):
                transaction_in_cycle : Transaction = self.transaction_table[start_node]
                if not youngest_transaction or transaction_in_cycle.begin_time > youngest_transaction.begin_time:
                    youngest_transaction = transaction_in_cycle

        # youngest_transaction found, abort it
        if youngest_transaction:
            print("Detected deadlock, abort transaction {}".format(youngest_transaction.transaction_id))
            self.abort(youngest_transaction.transaction_id)
            return True
        return False

