import cmd
import pandas as pd
from parse_query import parse_query



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



class CLI(cmd.Cmd):
    intro = 'Welcome to Roma Jay Data Base (RJDB) :D\n'
    prompt = 'RJDB >'
    
    def __init__(self): 
        super().__init__()
        self.storagesystem = StorageSystem()
        self.current_db = None
    
    def do_makedb(self, user_input_query):
            # storage_system = {"db1": {"table1": {table contents}, "table2": {table contents}}, "db2:" {...}} 
        '''
        Use this keyword when you want to create a new database and store it on the storage system.
        Syntax: MAKEDB dbname 
        ''' 
        self.storagesystem.add_db(user_input_query[1], {})
        
    def do_usedb(self, user_input_query):
        """
        Use this keyword at the beginning of a query to
        use an existing database.
        Syntax: USEDB dbname
        """
        dbname = user_input_query[1]
        if dbname in self.storagesystem.get_st(): 
            self.current_db = dbname 
            print(f"Switched to {dbname}")
        else: 
            print(f"DB not found")   
    
    def do_make(self, user_input_query):
        """
        Use this keyword at the beginning of a query
        to make a new table. 
        Syntax: MAKE (COPY) tablename (tablename2)
        """
        if "COPY" in user_input_query:
            table_name = user_input_query[2]
        else:
            table_name = user_input_query[1]
        current_db = self.storagesystem.get_db(self.current_db)
        current_db[table_name] = {parse_query(user_input_query)}
        
        print(pd.DataFrame(current_db[table_name]))
        
    def do_edit(self, user_input_query):
        """
        Use this keyword at the beginning of a query
        to edit an existing table, including insert, 
        update, and delete operations.
        General syntax: EDIT tablename INSERT/UPDATE/DELETE record
        Syntax for INSERT: EDIT tablename INSERT col1=x, col2=y, col3=z...
        Syntax for UPDATE: EDIT tablename UPDATE id=rownum, col3=abc, col5=xyz... 
        Syntax for DELETE: EDIT tablename DELETE id=rownum...
        """
        table_name = user_input_query[1] 
        current_db = self.storagesystem.get_db(self.current_db)
        if table_name in current_db: 
            current_db[table_name] = {parse_query(user_input_query)}
            print("Modified table: ", pd.DataFrame(current_db[table_name]))
        else:
            print(f"Table not found")
            
        print(pd.DataFrame(current_db[table_name]))
        
    
    def do_fetch(self, user_input_query): 
        """
        Use this keyword at the beginning of a query to 
        fetch existing table and peform aggregation, group by, having, sort, or merge operations 
        on specified columns. 
        Syntax: FETCH tablename [COLUMNS] [column(s)] [AGGREGATION FUNCTION] [column] [BUNCH/SORT/MERGE] [column] [HAS] [column(s)]
        Keywords should specifically be in the order of COLUMNS --> TOTALNUM/SUM/MEAN/MIN/MAX --> BUNCH --> SORT --> MERGE --> HAS.

        """
        parse_query(user_input_query)
        
    def do_delete(self, user_input_query): 
        table_name = user_input_query[1]
        current_db = self.storagesystem.get_db(self.current_db)
        
        if table_name in current_db: 
            del current_db[table_name]
        else: 
            print("Table not found")
    
    def do_exit(self):
        self.close() 
        print("Bye")



if __name__ == "__main__": 
    CLI().cmdloop()