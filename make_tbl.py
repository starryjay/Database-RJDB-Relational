import pandas as pd
import os

# Datatypes to account for: int64, str, float64, datetime64
global colnames
global dtypes

global dtype_dict 

def make(user_query_list):
    if user_query_list[0].upper() == "COPY":
        return make_copy(user_query_list[1:])
    elif user_query_list[1].upper() == "COLUMNS":
        tablename = user_query_list[0]
        for char in tablename:
            if char.isdigit():
                print("Please use a tablename without any numbers in it.")
                return
        if tablename in os.listdir(os.getcwd()):
            print("Table with name", tablename, "already exists! Please use a different name.")
            return
        columnstuples = [tuple(data.split('=')) for data in user_query_list[2:]]
        global colnames
        colnames = [columnstuples[i][0] for i in range(len(columnstuples))]
        global dtypes 
        dtypes = [columnstuples[i][1].lower() for i in range(len(columnstuples))]
        global dtype_dict
        dtype_dict = {"int": 'int64', "str": 'string', "float": 'float64', "datetime64": 'datetime64[ns]'}
        tbl = pd.DataFrame(columns=colnames)
        for colname, datatype in zip(colnames, dtypes):
            tbl[colname] = tbl[colname].astype(dtype_dict[datatype])
        tbl.reset_index(drop = True, inplace = True)
        tbl.name = tablename
        if not os.path.exists("./table"):
            os.mkdir("./table")
        tbl.to_pickle("./table/" + tbl.name + '.pkl')
        print("Successfully created table", tablename, "with columns", colnames, "and datatypes", dtypes)
    else:
        print("Please use keyword COPY or COLUMNS!")
        return
def make_copy(user_query_list):
    existingtable = user_query_list[0]
    copytable = user_query_list[1]
    if copytable in os.listdir(os.getcwd()):
        print("Table with name", copytable, "already exists! Please use a different name.")
        return
    curr_table = pd.read_pickle("./table/" + existingtable + '.pkl')
    copy = curr_table.copy(deep=False)
    copy.to_pickle("./table/" + copytable + '.pkl')
    print("Successfully created copy of table", existingtable, "called", copytable, "with columns", colnames, "and datatypes", dtypes)