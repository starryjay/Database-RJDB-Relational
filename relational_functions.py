from parse_query import parse_query, abspath
import pandas as pd
import os

# Datatypes to account for: int64, str, float64, datetime64
global colnames
global dtypes

global dtype_dict

def make(user_query_list): 
    if user_query_list[0].upper() == "COPY": 
        make_copy(user_query_list[1:])
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
        return tbl
def make_copy(user_query_list):
    existingtable = user_query_list[0]
    newtable = user_query_list[1]
    os.chdir('./table')
    curr_table = pd.read_feather(existingtable + '.feather')
    new = curr_table.copy(deep=False)
    new.to_feather(newtable + '.feather')
    return new

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

def fetch(user_query_list):
#get table name 
    table = pd.read_feather(user_query_list[0] + ".feather")
# if list length greater than 1, check following keyword
    if len(user_query_list) > 1: 
        kw = user_query_list[1]
        
                # check for additional keywords after list of columns
                
                # get appropriate columns based on what comes afterward
                # if additional keywords, push to appropriate function
# if totalnum, sum, mean, min, max, then get aggregation function 
# call aggregation function  
# if no totalnum/sum/mean/min/max, check for "columns"
# check for "has", "bunch", "sort", etc


def nm_kw_checker(user_query_list):
    if "HAS" in list(map(str.upper, user_query_list)):
            if "BUNCH" in list(map(str.upper, user_query_list)):
                bunchpos = list(map(str.upper, user_query_list)).index("BUNCH")
                condition = user_query_list[(list(map(str.upper, user_query_list)).index("HAS") + 1):bunchpos]
            if ("SORT" in list(map(str.upper, user_query_list))) and ("BUNCH" not in list(map(str.upper, user_query_list))):
                sortpos = list(map(str.upper, user_query_list)).index("SORT")
                condition = user_query_list[(list(map(str.upper, user_query_list)).index("HAS") + 1):sortpos]
            elif ("SORT" not in list(map(str.upper, user_query_list))) and ("BUNCH" not in list(map(str.upper, user_query_list))):
                condition = user_query_list[(list(map(str.upper, user_query_list)).index("HAS") + 1):]
            # parse columns and operator 
            columnname = condition[1] #price < 50 item < ""
            # SELECT * FROM a JOIN b ON a.col1 = b.col1 WHERE a.col1 > 5
            datatype = condition[]
            if "<" in condition: 
                less(table, columnname, )
                
            elif ">" in condition:
            elif "=" in condition:    
            
def m_kw_checker(user_query_list):
    if "HAS" in list(map(str.upper, user_query_list)):
            if "BUNCH" in list(map(str.upper, user_query_list)):
                bunchpos = list(map(str.upper, user_query_list)).index("BUNCH")
                condition = user_query_list[(list(map(str.upper, user_query_list)).index("HAS") + 1):bunchpos]
            if ("SORT" in list(map(str.upper, user_query_list))) and ("BUNCH" not in list(map(str.upper, user_query_list))):
                sortpos = list(map(str.upper, user_query_list)).index("SORT")
                condition = user_query_list[(list(map(str.upper, user_query_list)).index("HAS") + 1):sortpos]
            elif ("SORT" not in list(map(str.upper, user_query_list))) and ("BUNCH" not in list(map(str.upper, user_query_list))):
                condition = user_query_list[(list(map(str.upper, user_query_list)).index("HAS") + 1):]
            # parse columns and operator 
            columnname = condition[1] #price < 50 item < ""
            # SELECT * FROM a JOIN b ON a.col1 = b.col1 WHERE a.col1 > 5
            datatype = condition[]
            if "<" in condition: 
                less(table, columnname, )
                
            elif ">" in condition:
            elif "=" in condition:    

def agg_checker(user_query_list): 
    kw = user_query_list[1]
    if kw in ['totalnum','sum',' mean','min', 'max']: 
            res = aggregate(table, kw)
        elif kw not in ['totalnum','sum',' mean','min', 'max']: 
            if kw.lower() == "columns": 
    if agg_function.lower() == "totalnum":
        