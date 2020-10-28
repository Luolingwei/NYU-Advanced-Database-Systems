from Utils import Variable, CommitValue, TempValue, RW_Result, InvalidCommandError
from Locks import ReadLock, WriteLock, LockType, VarLockManager


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


    def read_snapshot(self, variable_id ,ts):
        v: Variable = self.data_table[variable_id]
        if v.is_readable:
            for commit_value in v.commit_queue:
                # get the latest commit before begin
                if commit_value.commit_ts <= ts:
                    if v.is_replicated:
                        for fail_ts in self.fail_time_list:
                            if commit_value.commit_ts < fail_ts <= ts:
                                return RW_Result(False)
                    return RW_Result(True, commit_value.value)
        return RW_Result(False)


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



    def abort(self, transaction_id):
        # release current locks
        for lockmgr in self.lock_table.values():
            lockmgr.release_current_lock_by_transaction(transaction_id)
            for locks in list(lockmgr.lock_queue):
                if locks.transaction_ids == transaction_id:
                    lockmgr.lock_queue.remove(locks)
        # update lock table.
        self.update_lock_table()



    def commit(self, transaction_id, commit_ts):
        # release locks for this transaction
        for lockmgr in self.lock_table.values():
            lockmgr.release_current_lock_by_transaction(transaction_id)
            for locks in list(lockmgr.lock_queue):
                if locks.transaction_ids == transaction_id:
                    raise RuntimeError("{} cannot commit with unresolved locks.".format(transaction_id))
        # update commit queue
        for val in self.data_table.values():
            if val.temp_value and val.temp_value.transaction_id == transaction_id:
                val.add_commit_value(CommitValue(val.temp_value.value, commit_ts))
                val.is_readable = True
        # update lock table.
        self.update_lock_table()


    def fail(self, fail_ts: int):
        """
        A site fail, set site status to down and reset lock manager for all vars in this site
        :param fail_ts: fail timestamp of this site
        """
        if not self.is_up:
            raise InvalidCommandError("Trying to fail a down site!")
        self.is_up = False
        self.fail_time_list.append(fail_ts)
        for var_lock_manager in self.lock_table.values():
            var_lock_manager.reset()


    def recover(self, recover_ts: int):
        """
        A site recover, set site status to down and set all replicated var to non-readable
        :param recover_ts: recover timestamp of this site
        """
        if self.is_up:
            raise InvalidCommandError("Trying to recover a up site!")
        self.is_up = True
        self.recover_time_list.append(recover_ts)
        for variable in self.data_table.values():
            # replicated variable (even index) not readable when site recover
            if variable.is_replicated:
                variable.is_readable = False



    def update_lock_table(self):
        # If the transaction has been aborted or committed, the related locks need to be moved
        for val, lockmgr in self.lock_table.items():
            if lockmgr.lock_queue:
                if not lockmgr.cur_lock:
                    first_lock = lockmgr.lock_queue.pop(0)
                    if first_lock.lock_type == LockType.R:
                        for trans_id in first_lock.transaction_ids:
                            lockmgr.cur_lock = ReadLock(first_lock.variable_id,trans_id)
                        for trans_id in first_lock.transaction_ids:
                            lockmgr.share_read_lock(trans_id)
                    else:
                        lockmgr.cur_lock = WriteLock(first_lock.variable_id, first_lock.transaction_ids)

                if lockmgr.cur_lock.lock_type == LockType.R:
                    for locks in list(lockmgr.lock_queue):
                        if locks.lock_type == LockType.W:
                            if len(lockmgr.cur_lock.transaction_ids) == 1 and locks.transaction_ids in lockmgr.cur_lock.transaction_ids:
                                # Promote the current lock from R-lock to W-lock
                                lockmgr.promote_current_lock(WriteLock(locks.variable_id, locks.transaction_ids))
                                lockmgr.lock_queue.remove(locks)
                            break
                        lockmgr.share_read_lock(locks.transaction_ids)
                        lockmgr.lock_queue.remove(locks)


    # Luo
    # return this site’s wait for graph for cycle detaction
    def get_blocking_graph(self):
        # output a waits-for all variables in current site
        pass

