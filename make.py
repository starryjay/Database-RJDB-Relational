import pandas as pd
import os
from cli import storage_system

# Datatypes to account for: int64, str, float64, datetime64
global colnames
global dtypes

global dtype_dict

def make(user_query_list): 
    if user_query_list[0].upper() == "COPY": 
        return make_copy(user_query_list[1:])
    elif user_query_list[1].upper() == "COLUMNS":
        tablename = user_query_list[0]
        columnstuples = [tuple(data.split('=')) for data in user_query_list[2:]]
        global colnames
        colnames = [columnstuples[i][0] for i in range(len(columnstuples))]
        global dtypes 
        dtypes = [columnstuples[i][1].lower() for i in range(len(columnstuples))]
        tbl = pd.DataFrame(columns=colnames)
        tbl.reset_index(drop = True, inplace = True)
        tbl.name = tablename
        tbl.to_feather(tbl.name + '.feather')
        storage_system[tbl.name] = tbl
        return tbl
def make_copy(user_query_list):
    existingtable = user_query_list[0]
    newtable = user_query_list[1]
    os.chdir('./table')
    curr_table = pd.read_feather(existingtable + '.feather')
    new = curr_table.copy(deep=False)
    new.to_feather(newtable + '.feather')
    storage_system[new.name] = new
    return new