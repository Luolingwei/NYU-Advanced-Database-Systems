from collections import defaultdict
from Utils import Variable, CommitValue, TempValue, RW_Result, InvalidCommandError
from Locks import Lock, ReadLock, WriteLock, LockType, VarLockManager


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


    def read(self, transaction_id: str, variable_id: str):
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


    def read_snapshot(self, variable_id: str, begin_ts: int):
        """
        A read-only transaction T want to read a variable i from this site
        :param variable_id: id of variable which this transaction wants to read from
        :param begin_ts: timestamp when transaction T begin
        :return: RW_Result. True means read success, and a value is in RW_Result. False means read fail, no value
        """
        variable: Variable = self.data_table[variable_id]
        if variable.is_readable:
            # get the latest commit before transaction's begin time
            # our latest commit value exist in the tail of commit queue
            for commit_value in variable.commit_queue[::-1]:
                # we found the snapshot
                if commit_value.commit_ts <= begin_ts:
                    if variable.is_replicated:
                        for fail_time in self.fail_time_list:
                            # if site ever failed after the commit and before the transaction's begin time
                            # this snapshot is invalid
                            if commit_value.commit_ts < fail_time <= begin_ts:
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


    def write(self, transaction_id: str, variable_id: str, value: int):
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


    def abort(self, transaction_id: str):
        """
        A transaction abort, release all current locks / queued locks held by it
        :param transaction_id: id of the transaction to be aborted
        """
        # release all current locks for this transaction
        for lock_mgr in self.lock_table.values():
            lock_mgr.release_lock_held_by_transaction(transaction_id)
            # release all queued lock for this transaction
            for lock in list(lock_mgr.lock_queue):
                # queued lock can only have 1 transaction on it
                if lock.lock_type == LockType.R and transaction_id in lock.transaction_ids:
                    lock_mgr.lock_queue.remove(lock)
                if lock.lock_type == LockType.W and transaction_id == lock.transaction_ids:
                    lock_mgr.lock_queue.remove(lock)

        # update lock table.
        self.update_lock_table()


    def commit(self, transaction_id: str, commit_ts: int):
        """
        A transaction commit, release all current lock held by it. Commit all temp value to commit queue
        If this transaction has queued lock, we can not commit it with queued locks
        :param transaction_id: id of the transaction to be committed
        :param commit_ts: timestamp when this commit happen
        """
        # release all current locks for this transaction
        for lock_mgr in self.lock_table.values():
            lock_mgr.release_lock_held_by_transaction(transaction_id)
            # detect whether there is queued lock for this transaction
            for lock in lock_mgr.lock_queue:
                # queued lock can only have 1 transaction on it
                if lock.lock_type == LockType.R and transaction_id in lock.transaction_ids:
                    raise RuntimeError("{} cannot commit with queued locks: {}".format(transaction_id, lock))
                if lock.lock_type == LockType.W and transaction_id == lock.transaction_ids:
                    raise RuntimeError("{} cannot commit with queued locks: {}".format(transaction_id, lock))

        # update commit queue
        for variable in self.data_table.values():
            if variable.temp_value and variable.temp_value.transaction_id == transaction_id:
                variable.add_commit_value(CommitValue(variable.temp_value.value, commit_ts))
                variable.is_readable = True

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
        """
        After commit/abort, some locks are released, move queued lock to var is possible.
        """
        for lock_mgr in self.lock_table.values():
            if lock_mgr.lock_queue:
                current_lock = lock_mgr.cur_lock
                # current lock has become empty, need to move first queued lock onto it
                if not current_lock:
                    first_queued_lock: Lock = lock_mgr.lock_queue.popleft()
                    first_queued_lock.is_queued = False
                    lock_mgr.cur_lock = first_queued_lock

                # current lock is read lock
                # check whether this lock can be shared by later read lock, or promote to write lock
                if lock_mgr.cur_lock.lock_type == LockType.R:
                    while lock_mgr.lock_queue:
                        # fetch queued lock one by one
                        next_lock = lock_mgr.lock_queue[0]
                        # successive read lock, can share
                        if next_lock.lock_type == LockType.R:
                            lock_mgr.share_read_lock(list(next_lock.transaction_ids)[0])
                            lock_mgr.lock_queue.popleft()
                        # once we meet write lock, share have to stop, as read lock can not skip write lock in queue
                        elif next_lock.lock_type == LockType.W:
                            # now check whether the read lock has not been shared, if so, it's possible to promote
                            if len(lock_mgr.cur_lock.transaction_ids) == 1 and next_lock.transaction_ids in lock_mgr.cur_lock.transaction_ids:
                                # the current lock is read lock and has same transaction id with later write lock, promote
                                # set this write lock as current lock
                                next_lock.is_queued = False
                                lock_mgr.cur_lock = next_lock
                                lock_mgr.lock_queue.popleft()
                            # stop scanning
                            break


    def get_wait_for_graph(self):
        """
        Calculate all wait-for relations in this site
        including wait-for between (1) current lock and queued lock, (2) queued lock and queued lock
        :return: return this site’s wait for graph for cycle detection
        """

        def check(lock_left, lock_right):
            # lock_left is read lock
            if lock_left.lock_type == LockType.R:
                # lock_right is write lock but not the same transaction id, which will cause wait
                if lock_right.lock_type == LockType.W and \
                        {lock_right.transaction_ids} != lock_left.transaction_ids:
                    # only add transaction ids which != write lock's transaction id on the right side
                    wait_for_graph[lock_right.transaction_ids] |= {id for id in lock_left.transaction_ids if id!=lock_right.transaction_ids}

            # lock_left is write lock
            else:
                # only need to judge whether the 2 transaction id is the same, if not, will cause wait
                # lock_left is write lock, lock_right is read lock (queued read lock can only have 1 T shared)
                if lock_right.lock_type == LockType.R and lock_left.transaction_ids not in lock_right.transaction_ids:
                    wait_for_graph[list(lock_right.transaction_ids)[0]].add(lock_left.transaction_ids)
                # lock_left is write lock, lock_right is write lock
                elif lock_right.lock_type == LockType.W and lock_left.transaction_ids != lock_right.transaction_ids:
                    wait_for_graph[lock_right.transaction_ids].add(lock_left.transaction_ids)

        wait_for_graph = defaultdict(set)

        for var_lock_manager in self.lock_table.values():
            current_lock = var_lock_manager.cur_lock
            lock_queue = var_lock_manager.lock_queue

            if current_lock:
                # Calculate wait-for between current lock and queued lock
                for queued_lock in var_lock_manager.lock_queue:
                    check(current_lock, queued_lock)
            if lock_queue:
                # Calculate wait-for between queued lock and queued lock
                for j in range(len(lock_queue)):
                    for i in range(j):
                        check(lock_queue[i],lock_queue[j])

        if len(wait_for_graph.keys())>0: print("wait-for graph for site {}: {}".format(self.site_id, wait_for_graph))
        return wait_for_graph
