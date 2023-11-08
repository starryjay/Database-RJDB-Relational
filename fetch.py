import pandas as pd
import os

# Datatypes to account for: int64, str, float64, datetime64
global colnames
global dtypes

global dtype_dict

# [tablename, COLUMNS, columns, TOTALNUM/SUM/MEAN/MIN/MAX, column, 
# BUNCH, columns, SORT, columns, ASC/DESC, MERGE, tablename2, HAS, conditions]

'''

if COLUMNS keyword and aggregate function keyword both present, there must be a BUNCH keyword as well 
which bunches by columns present after COLUMNS keyword
in which case the table returned will be grouped by non-aggregated columns with aggregated columns for each group

'''

def fetch(user_query_list):
    global dtype_dict
    dtype_dict = {"int64": int, "str": str, "float64": float, "datetime64": 'datetime64[ns]'}
    table = pd.read_feather(user_query_list[0] + ".feather")
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if user_query_list[1].upper() == "COLUMNS":
        table = table.loc[:, get_columns(user_query_list)]
    if (not set(agglist).isdisjoint(set(user_query_list))):
        table = agg_functions(user_query_list, table)
    else:
        if "BUNCH" in user_query_list:
            table = bunch(user_query_list, table)
        if "SORT" in user_query_list:
            table = sort(user_query_list, table)
        if "MERGE" in user_query_list:
            table = merge(user_query_list, table)
        if "HAS" in user_query_list:
            table = has(user_query_list, table)
    return table

def get_columns(user_query_list):
    kwlist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX", "BUNCH", "SORT", "MERGE", "HAS"]
    idxlist = {}
    for kw in kwlist:
        if kw in user_query_list:
            idxlist[user_query_list.index(kw)] = kw
        nextkwidx = min(idxlist)
    cols_list = user_query_list[2:nextkwidx]
    return cols_list

def agg_functions(user_query_list, table): 
    if "SUM" in list(map(str.upper, user_query_list)):
        return tblsum(user_query_list, table) 
    elif "TOTALNUM" in list(map(str.upper, user_query_list)):
        return totalnum(user_query_list, table)
    elif "MEAN" in list(map(str.upper, user_query_list)):
        return mean(user_query_list, table)
    elif "MIN" in list(map(str.upper, user_query_list)):
        return tblmin(user_query_list, table)
    elif "MAX" in list(map(str.upper, user_query_list)):
        return tblmax(user_query_list, table)

def tblsum(user_query_list, table):
    col = list(map(str.upper, user_query_list)).index("SUM") + 1
    if (table.dtypes[col] != pd.Int64Dtype) and (table.dtypes[col] != pd.Float64Dtype):
        raise ValueError("SUM can only be used on a numeric column")
    else:
        return sum(list(table[col]))

def totalnum(user_query_list, table):
    col = list(map(str.upper, user_query_list)).index("TOTALNUM") + 1
    return len(table[col])

def mean(user_query_list, table):
    col = list(map(str.upper, user_query_list)).index("MEAN") + 1
    if (table.dtypes[col] != pd.Int64Dtype) and (table.dtypes[col] != pd.Float64Dtype):
        raise ValueError("SUM can only be used on a numeric column")
    else:
        return (sum(list(table[col]))/len(table[col]))

def tblmin(user_query_list, table):
    col = list(map(str.upper, user_query_list)).index("MIN") + 1
    return min(table[col])

def tblmax(user_query_list, table):
    col = list(map(str.upper, user_query_list)).index("MAX") + 1
    return max(table[col])

def bunch(user_query_list, table):
    bunchidx = list(map(str.upper, user_query_list)).index("BUNCH") + 1
    groups = table[bunchidx].unique()
    table = pd.DataFrame({grp: table[bunchidx] == grp for grp in groups})
    return table

def sort(user_query_list, table):
    sortcol = list(map(str.upper, user_query_list)).index("SORT") + 1
    if "ASC" in user_query_list:
       return ascSort(table, sortcol)
    elif "DESC" in user_query_list:
        return descSort(table, sortcol)
    else:
        raise ValueError('Direction keyword missing, please specify direction to sort as ASC or DESC!')

def ascSort(table, sortcol):
    
    return table

def descSort(table, sortcol):

    return table

def merge(user_query_list, table1, table2):
    return

def has(user_query_list, table):
    user_query_list = list(map(str.upper, user_query_list))
    condidx = user_query_list.index("HAS") + 1
    cond = user_query_list[condidx]
    if "<" in cond:
        cond = user_query_list[condidx].split("<")
        col1 = cond[0]
        col2 = cond[2]
        table = table.loc[table[col1] < table[col2]]
    elif ">" in cond:
        cond = user_query_list[condidx].split(">")
        col1 = cond[0]
        col2 = cond[2]
        table = table.loc[table[col1] > table[col2]]
    elif "=" in cond:
        cond = user_query_list[condidx].split("=")
        col1 = cond[0]
        col2 = cond[2]
        table = table.loc[table[col1] == table[col2]]
    return table