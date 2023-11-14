class StorageSystem(): 
    
    def __init__(self, st={}):
        self.st = st

    def get_st(self):
        return self.st
    
    def set_st(self, new_st):
        self.st = new_st
    
    def get_db(self, db):
        return self.st[db]
    
    def add_db(self, dbname, db):
        self.st[dbname] = db

    def del_db(self, dbname):
        del self.st[dbname]