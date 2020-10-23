from enum import Enum, unique

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


@unique
class LockType(Enum):
    R = 0
    W = 1


class Lock:

    def __init__(self, variable_id: str, is_queued: bool, lock_type: LockType):
        """
        Initialize a Lock Object, it has 2 subclasses ReadLock and WriteLock
        :param variable_id: id of variable which this lock belongs to
        :param is_queued: indicate whether this lock is in lock_queue of VarLockManager
        :param lock_type: could be R or W, represent ReadLock or WriteLock
        """
        self.variable_id = variable_id
        self.transaction_ids = None # Read Lock can be shared by multiple transactions
        self.lock_type = lock_type
        self.is_queued = is_queued


    def __repr__(self):
        """
        Output lock object info
        """
        return "Lock [{}, {}, {}, {}]".format(self.variable_id, self.transaction_ids, self.lock_type, self.is_queued)


class ReadLock(Lock):

    def __init__(self, variable_id, transaction_id, is_queued = False):
        """
        Inherit from Lock
        :param variable_id: id of variable which this lock belongs to
        :param transaction_id: id of transaction which obtain this ReadLock
        :param is_queued: indicate whether this lock is in lock_queue of VarLockManager
        """
        super().__init__(variable_id, is_queued, LockType.R)
        self.transaction_ids = {transaction_id} # Read Lock can be shared by multiple transactions


class WriteLock(Lock):

    def __init__(self, variable_id, transaction_id, is_queued = False):
        """
        Inherit from Lock
        :param variable_id: id of variable which this lock belongs to
        :param transaction_id: id of transaction which obtain this WriteLock
        :param is_queued: indicate whether this lock is in lock_queue of VarLockManager
        """
        super().__init__(variable_id, is_queued, LockType.W)
        self.transaction_ids = transaction_id # There can only be 1 Write lock on 1 var


class VarLockManager:

    def __init__(self, variable_id: str):
        """
        Initialize VarLockManager Object
        :param variable_id: indicate this lock manager belong to which variable
        """
        self.variable_id = variable_id
        self.cur_lock: Lock = None # cuurent lock on this variable, can be ReadLock or WriteLock
        self.lock_queue = [] # a queue to store all failed attempted lock


    def reset(self):
        """
        Reset the status of this VarLockManager to initial status
        :return:
        """
        self.cur_lock = None
        self.lock_queue = []


    def has_queued_write_lock(self, exclude_transaction_id = None):
        """
        Judge whether there is queued write lock for this variable
        :param exclude_transaction_id: if exclude_transaction_id is passed in, ignore locks belogn to this transaction
        :return: True/False means whether there is queued write lock
        """
        for lock in self.lock_queue:
            if lock.lock_type == LockType.W:
                if exclude_transaction_id and lock.transaction_ids == exclude_transaction_id:
                    continue
                return True
        return False


    def add_lock_to_queue(self, lock_to_add: Lock):
        """
        Add a new lock into lock queue of this variable
        Under 2 cases we will not add the lock to queue:
        (1) this same lock already exists in the lock queue
        (2) lock_to_add is ReadLock, and the WriteLock of same transaction and var already exists
        :param lock_to_add: the lock waited to be added
        """
        for lock in self.lock_queue:
            if lock.transaction_ids == lock_to_add.transaction_ids:
                if lock.lock_type == lock_to_add.lock_type or lock_to_add.lock_type == LockType.R:
                    return
        self.lock_queue.append(lock_to_add)


    def share_read_lock(self, transaction_id):
        """
        A transaction share this lock with other existing transactions
        :param transaction_id: newly joined transaction
        """
        if self.cur_lock.lock_type != LockType.R:
            raise RuntimeError("Trying to share a write lock!!!")
        self.cur_lock.transaction_ids.add(transaction_id)


# A DataManager represents a site, where all variables and locks would be stored here.
class DataManager:


    def __init__(self, site_id: int):
        """
        Initialize DataManager Object
        Initialize variables in each site, create data table and lock table
        :param site_id: id of this site
        """
        self.is_up = True
        self.site_id = site_id
        self.data_table = {} # {variable id: Variable}
        self.lock_table = {} # {variable id: VarLockManager}
        self.fail_time_list = [] # latest fail time will be append at tail
        self.recover_time_list = [] # latest recover time will be append at tail

        # add all variables which belong to this site
        for num in range(1,21):
            var_id = 'x' + str(num)
            # Even indexed variables are at all sites
            if num%2 == 0:
                self.lock_table[var_id] = VarLockManager(var_id)
                self.data_table[var_id] = Variable(var_id, CommitValue(10*num, 0), True)
            # The odd indexed variables are at one site each
            elif num%10 + 1 == self.site_id:
                self.lock_table[var_id] = VarLockManager(var_id)
                self.data_table[var_id] = Variable(var_id, CommitValue(10*num, 0), False)


    def has_variable(self, variable_id: str):
        """
        Judge whether a variable id exists in this site
        :param variable_id: id of the variable, like x3
        :return: True means variable_id exists, False means not exist
        """
        return variable_id in self.data_table


    def dump(self):
        """
        Output all useful info about this site
        Print site status, latest commit value for all variables
        """
        status = "UP" if self.is_up else "DOWN"
        site_info = "site {} [{}] site info - ".format(self.site_id, status)
        lock_info = "site {} [{}] lock info - ".format(self.site_id, status)

        for variable in self.data_table.values():
            site_info += "{}: {}, ".format(variable.variable_id, variable.get_latest_commit_value())
            current_lock = self.lock_table.get(variable.variable_id).cur_lock
            if current_lock: lock_info += "{}: {}, ".format(variable.variable_id, current_lock)
        print(site_info)
        print(lock_info)


    def read(self, transaction_id, variable_id):
        """
        A transaction T want to read a variable i from this site
        First judge the current lock type on this variable, then try to get read lock of this variable
        :param transaction_id: id of this transaction which wants to do read
        :param variable_id: id of variable which this transaction wants to read from
        :return: RW_Result. True means read success, and a value is in RW_Result. False means read fail, no value
        """

        variable: Variable = self.data_table.get(variable_id)
        var_lock_manager: VarLockManager = self.lock_table.get(variable_id)

        current_lock = var_lock_manager.cur_lock
        if not variable.is_readable: return RW_Result(False)

        # currently, var has been locked
        if current_lock:

            if current_lock.lock_type == LockType.R:
                # current transaction already get a read lock on var
                if transaction_id in current_lock.transaction_ids:
                    return RW_Result(True, variable.get_latest_commit_value())
                # check if there is queued write lock, if not, share read lock with existing transactions
                if not var_lock_manager.has_queued_write_lock():
                    var_lock_manager.share_read_lock(transaction_id)
                    return RW_Result(True, variable.get_latest_commit_value())
                # there is queued write lock, can not skip it to obtain read lock, add this read lock to lock queue
                var_lock_manager.add_lock_to_queue(ReadLock(variable_id, transaction_id, True))
                return RW_Result(False)

            elif current_lock.lock_type == LockType.W:
                # current transaction already get a write lock on var, temp value has been written
                if current_lock.transaction_ids == transaction_id:
                    return RW_Result(True, variable.get_temp_value())
                # other transaction is holding a write lock on var, add this read lock to lock queue
                else:
                    var_lock_manager.add_lock_to_queue(ReadLock(variable_id, transaction_id, True))
                    return RW_Result(False)

        # var has no lock on it
        else:
            var_lock_manager.cur_lock = ReadLock(variable_id,transaction_id)
            return RW_Result(True, variable.get_latest_commit_value())


    # a read-only transaction T want to read a snapshot of variable i from this site
    def read_snapshot(self, transaction_id, variable_id):
        # find the latest commit value of this variable before begin time of T in commit queue.
        pass


    def can_get_write_lock(self, transaction_id: str, variable_id: str):
        """
        Judge whether write lock of variable_id can be obtained from this site
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to write
        :return: True / False means can/can't get write lock
        """

        variable: Variable = self.data_table.get(variable_id)
        var_lock_manager: VarLockManager = self.lock_table.get(variable_id)

        current_lock = var_lock_manager.cur_lock

        # currently, var has been locked
        if current_lock:

            if current_lock.lock_type == LockType.R:
                # current transaction already get a read lock on var, judge whether it is shared
                if len(current_lock.transaction_ids)>1: # shared with other T, can not write
                    var_lock_manager.add_lock_to_queue(WriteLock(variable_id, transaction_id, True))
                    return False
                if transaction_id not in current_lock.transaction_ids: # hold by other T, can not write
                    var_lock_manager.add_lock_to_queue(WriteLock(variable_id, transaction_id, True))
                    return False

                # read lock hold only by this transaction, try to promote its lock from read to write

                # have other T's queued write lock, can not skip
                if var_lock_manager.has_queued_write_lock(transaction_id):
                    var_lock_manager.add_lock_to_queue(WriteLock(variable_id, transaction_id, True))
                    return False
                # can promote read lock to write lock
                return True

            elif current_lock.lock_type == LockType.W:
                # current transaction already get a write lock on var, temp value has been written
                if current_lock.transaction_ids == transaction_id:
                    return True
                # other transaction is holding a write lock on var, can not write
                else:
                    var_lock_manager.add_lock_to_queue(WriteLock(variable_id, transaction_id, True))
                    return False

        # var has no lock on it
        else:
            return True


    def write(self, transaction_id, variable_id, value):
        """
        A transaction T want to write a variable i to value V in this site
        As write operation would be first judged by can_get_write_lock, so when we do write,
        all write lock can must be obtained, we can safely write value to var and set new write lock.
        :param transaction_id: id of this transaction
        :param variable_id: id of variable which T wants to write
        :param value: value which T wants to write to var, will be write to temp value
        :return: When we do write, write can be guaranteed to be success, so we always return RW_Result(True)
        """
        variable: Variable = self.data_table.get(variable_id)
        var_lock_manager: VarLockManager = self.lock_table.get(variable_id)

        # Write value to temp_value of var, which will be committed when T commit
        variable.temp_value = TempValue(value, transaction_id)
        # Safely set new lock to this write lock
        var_lock_manager.cur_lock = WriteLock(variable_id, transaction_id)
        return RW_Result(True)


    # a transaction is aborted, do corresponding operations in current site
    def abort(self, transaction_id):
        # release current locks and queued locks of this transaction
        pass


    # a transaction is committed, do corresponding operations in current site
    def commit(self, transaction_id):
        # Add all temp value of this transaction in this site to commit list, release all current and queued locks.
        pass


    # a site fail, do corresponding operations in current site
    def fail(self, fail_ts):
        """
        A site fail, set site status to down and reset lock manager for all vars in this site
        :param fail_ts: fail timestamp of this site
        """
        self.is_up = False
        self.fail_time_list.append(fail_ts)
        for var_lock_manager in self.lock_table.values():
            var_lock_manager.reset()


    # a site recover, do corresponding operations in current site
    def recover(self, recover_ts):
        # Set site status to up
        pass


    # return this site’s wait for graph for cycle detaction
    def get_blocking_graph(self):
        # output a waits-for all variables in current site
        pass
