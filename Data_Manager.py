
class CommitValue:

    def __init__(self, value: int, commit_ts: int):
        """
        Initialize CommitValue Object
        :param value: actual value
        :param commit_ts: commit timestamp of the value
        """
        self.value = value # actual value of this commit value in commit_queue
        self.commit_ts = commit_ts # commit timestamp of this value


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


class VarLockManager:

    def __init__(self, variable_id: str):
        """
        Initialize VarLockManager Object
        :param variable_id: indicate this lock manager belong to which variable
        """
        self.variable_id = variable_id
        self.cur_lock = None
        self.lock_queue = []


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
        self.data_table = {}
        self.lock_table = {}

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


    def dump(self):
        """
        Output all useful info about this site
        Print site status, latest commit value for all variables
        """
        status = "UP" if self.is_up else "DOWN"
        out = "site {} [{}] - ".format(self.site_id, status)

        for variable in self.data_table.values():
            out += (variable.variable_id + ": " + str(variable.commit_queue[-1].value) + ", ")
        print(out)


    # a transaction T want to read a variable i from this site
    def read(self, transaction_id, variable_id):
        # First judge the current lock type on this variable, then try to get read lock of this variable,
        # return True or False, which indicate whether this read is success or fail
        pass

    # a read-only transaction T want to read a snapshot of variable i from this site
    def read_snapshot(self, transaction_id, variable_id):
        # find the latest commit value of this variable before begin time of T in commit queue.
        pass


    # a transaction T want to write a variable i to value V in this site
    def write(self, transaction_id, variable_id, value):
        # First judge the current lock type on this variable, then try to get write lock of this variable,
        # return True or False, which indicate whether this write is success or fail
        pass


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
        # Set site status to down and clear lock table in this site
        pass


    # a site recover, do corresponding operations in current site
    def recover(self, recover_ts):
        # Set site status to up
        pass


    # return this site’s wait for graph for cycle detaction
    def get_blocking_graph(self):
        # output a waits-for all variables in current site
        pass
