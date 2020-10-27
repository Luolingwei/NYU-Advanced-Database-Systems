from enum import Enum, unique


#####################################################################
###################### Utils for Data Manager #######################
#####################################################################

class CommitValue:

    def __init__(self, value: int, commit_ts: int):
        """
        Initialize CommitValue Object
        :param value: actual value
        :param commit_ts: commit timestamp of the value
        """
        self.value = value # actual value of this commit value in commit_queue
        self.commit_ts = commit_ts # commit timestamp of this value


class TempValue:

    def __init__(self, value: int, transaction_id: str):
        """
        :param value: actual value
        :param transaction_id: id of transaction which writes this temp value
        """
        self.value = value
        self.transaction_id = transaction_id


class Variable:

    def __init__(self, variable_id: str, initial_value: CommitValue, is_replicated: bool):
        """
        Initialize Variable Object
        :param variable_id: id of this variable
        :param initial_value: value assigned in initialization, will be added to commit queue
        :param is_replicated: whether this variable is replicated, even index var is replicated
        """
        self.variable_id = variable_id
        self.commit_queue = [initial_value] # later commit value will be append to the tail
        self.is_replicated = is_replicated
        self.temp_value: TempValue = None # temporary value which has been writen but not committed
        self.is_readable = True # replicated variable (even index) not readable when site recover


    def get_latest_commit_value(self):
        """
        Get the latest commit value for this variable
        return: latest commit value of this variable (Int)
        """
        if not self.commit_queue:
            raise RuntimeError("commit queue of {} is empty".format(self.variable_id))
        return self.commit_queue[-1].value


    def get_temp_value(self):
        """
        Get the temp value for this variable,
        temp value means the value has been writen to var, but has not been committed
        :return: temp value of this variable (Int)
        """
        if self.temp_value == None:
            raise RuntimeError("temp value of {} doesn't exist".format(self.variable_id))
        return self.temp_value.value


class RW_Result:

    def __init__(self, success: bool, value = None):
        """
        Initialize Return Result Object, could be read result / write result
        :param success: indicate whether the read / write succeed
        :param value: only for read, the value got from site
        """
        self.success = success
        self.value = value


#####################################################################
################### Utils for Transaction Manager ###################
#####################################################################

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
