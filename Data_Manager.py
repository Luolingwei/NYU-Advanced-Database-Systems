
# A DataManager represents a site, where all variables and locks would be stored here.
class DataManager:


    # initialize variables in each site, create data table and lock table
    def __init__(self, site_id):
        # finish initialization of site
        pass


    # output all useful info about this site
    def dump(self):
        # print site status, list of commit value for all variables
        pass


    # a transaction T want to read a variable i from this site
    def read(self, transaction_id, variable_id):
        # First judge the current lock type on this variable, then try to get read lock of this variable,
        # return True or False, which indicate whether this read is success or fail
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
