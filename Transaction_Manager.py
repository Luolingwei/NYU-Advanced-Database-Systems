import re


class InvalidInstructionError(Exception):
    """Error thrown when the instruction is invalid."""

    def __init__(self, message):
        self.message = message


class Transaction:

    def __init__(self):
        pass



# Transaction Manager is used to process all instructions and conduct corresponding operations for various transactions
class TransactionManager:

    # initialize Transaction Manager
    def __init__(self):
        # call DataManager to finish initialization of all sites
        pass


    # output all useful info about all sites
    def dump(self):
        #call DM to print info about each site
        pass


    # process a line from input file
    def process_line(self, line):
        # return True or False, which indicate whether this line is processed correctly
        # call process_command to do corresponding operations
        paras = re.findall(r"[\w']+", line)
        print(paras)
        command = paras.pop(0)
        self.process_command(command, paras)


    # do corresponding operation according to the command
    # ["begin", "beginRO", "read", "write", "dump", "end", "fail", "recover"]
    def process_command(self, command, paras):
        # do operations according to command
        if command in("begin", "beginRO"):
            self.beigin(paras[0])
        elif command == "R":
            self.read(paras[0],paras[1])
        elif command == "W":
            self.write(paras[0],paras[1],paras[2])
        elif command == "dump":
            self.dump()
        elif command == "end":
            self.end(paras[0])
        elif command == "fail":
            self.fail(paras[0])
        elif command == "recover":
            self.recover(paras[0])
        else:
            raise InvalidInstructionError("Unknown Instruction: " + command)


    # a transaction T want to read a variable i
    def read(self, transaction_id, variable_id):
        # call DM to read from any sites which have this variable
        # return True or False, which indicate whether this read is success or fail
        pass


    # a transaction T want to write a variable i to value X
    def write(self, transaction_id, variable_id, value):
        # call DM to write to all up sites, as long as write lock can be acquired,
        # return True or False, which indicate whether this write is success or fail
        pass


    # a transaction is about to begin, do corresponding operations
    def beigin(self, transaction_id):
        # Initialize this transaction with current timestamp and add it into transaction table.
        pass


    # a transaction is about to end, do corresponding operations
    def end(self, transaction_id):
        # if the abort flag of this transaction is true, abort it. Otherwise commit it.
        pass


    # a transaction is about to abort, do corresponding operations
    def abort(self, transaction_id):
        # call DM to abort this transaction, remove transaction id from transaction table in TM.
        pass


    # a transaction is about to commit, do corresponding operations
    def commit(self, transaction_id):
        # call DM to commit this transaction, remove transaction id from transaction table in TM.
        pass


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
