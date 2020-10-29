from enum import Enum, unique
from collections import deque


############################################################
########### Definition for Lock and Lock Manager ###########
############################################################

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

    def __init__(self, variable_id: str, transaction_id: str, is_queued = False):
        """
        Inherit from Lock
        :param variable_id: id of variable which this lock belongs to
        :param transaction_id: id of transaction which obtain this ReadLock
        :param is_queued: indicate whether this lock is in lock_queue of VarLockManager
        """
        super().__init__(variable_id, is_queued, LockType.R)
        self.transaction_ids = {transaction_id} # Read Lock can be shared by multiple transactions, so use set here


class WriteLock(Lock):

    def __init__(self, variable_id: str, transaction_id: str, is_queued = False):
        """
        Inherit from Lock
        :param variable_id: id of variable which this lock belongs to
        :param transaction_id: id of transaction which obtain this WriteLock
        :param is_queued: indicate whether this lock is in lock_queue of VarLockManager
        """
        super().__init__(variable_id, is_queued, LockType.W)
        self.transaction_ids = transaction_id # There can only be 1 Write lock on 1 var, so didn't use set


class VarLockManager:

    def __init__(self, variable_id: str):
        """
        Initialize VarLockManager Object
        :param variable_id: indicate this lock manager belong to which variable
        """
        self.variable_id = variable_id
        self.cur_lock: Lock = None # cuurent lock on this variable, can be ReadLock or WriteLock
        self.lock_queue = deque() # a queue to store all failed attempted lock


    def reset(self):
        """
        Reset the status of this VarLockManager to initial status
        :return:
        """
        self.cur_lock = None
        self.lock_queue = deque()


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


    def share_read_lock(self, transaction_id: str):
        """
        A transaction share this lock with other existing transactions
        :param transaction_id: newly joined transaction
        """
        if self.cur_lock.lock_type != LockType.R:
            raise RuntimeError("Trying to share a write lock!!!")
        self.cur_lock.transaction_ids.add(transaction_id)


    def release_lock_held_by_transaction(self, transaction_id: str):
        """
        Release all current lock held by a transaction.
        :param transaction_id: the id of the transaction
        """
        # No current lock
        if not self.cur_lock: return

        # current lock is read lock
        if self.cur_lock.lock_type == LockType.R:

            if transaction_id in self.cur_lock.transaction_ids:
                self.cur_lock.transaction_ids.remove(transaction_id)
            # current lock has become empty, release it
            if len(self.cur_lock.transaction_ids) == 0:
                self.cur_lock = None

        # current lock is write lock
        else:
            if self.cur_lock.transaction_ids == transaction_id:
                self.cur_lock = None
