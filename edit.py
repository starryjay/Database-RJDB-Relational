import pandas as pd
import os

# Datatypes to account for: int64, str, float64, datetime64
global colnames
global dtypes

global dtype_dict

abspath = "/Users/roma/Documents/USC/Fall 2023 /Data Management /DSCI-551-Final-Proj-Rel"

def edit(user_query_list):
    global dtype_dict
    dtype_dict = {"int64": int, "str": str, "float64": float, "datetime64": 'datetime64[ns]' }
    if not os.path.exists(abspath + '/table/' + user_query_list[0] + ".feather"):
        raise ValueError("Invalid query - tablename does not exist!")
    if user_query_list[1].upper() == 'INSERT':
        return insert(user_query_list)
    elif user_query_list[1].upper() == 'DELETE':
        return delete(user_query_list)
    elif user_query_list[1].upper() == "UPDATE": 
        return update(user_query_list)

def insert(user_query_list):
    table = pd.read_feather(user_query_list[0] + ".feather")
    s = pd.Series(dict([tuple(data.split('=')) for data in user_query_list[2:]]))
    table.loc[len(table)] = s
    for colname, dtype in zip(colnames, dtypes):
        global dtype_dict
        table.astype({colname: dtype_dict[dtype.lower()]}, copy=False)
    table.to_feather(user_query_list[0] + ".feather")
    print(table)
    return table
 
def update(user_query_list):  
    table = pd.read_feather(user_query_list[0] + ".feather")
    listoftuples = [tuple(data.split('=')) for data in user_query_list[2:]]
    rownum = int(listoftuples[0][1])
    modcols = [i[0] for i in listoftuples[1:]]
    for i in listoftuples[1:]:
        table.loc[rownum, i[0]] = i[1]
    for i in modcols:
        for j, k in zip(colnames, dtypes):
            if i == j:
                table[i].astype(k, copy = False)
    return table

def delete(user_query_list):
    table = pd.read_feather(user_query_list[0] + ".feather")
    rownum = int(user_query_list[2][3:])
    table.drop(rownum, axis='index', inplace=True)
    return table