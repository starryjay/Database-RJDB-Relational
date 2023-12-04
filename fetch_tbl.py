import pandas as pd
import numpy as np
import os
import warnings
from printoutput import find_directory
warnings.simplefilter(action='ignore', category=FutureWarning)

# [tablename, COLUMNS, columns, TOTALNUM/SUM/MEAN/MIN/MAX, column, 
# BUNCH, columns, SORT, columns, ASC/DESC, MERGE, tablename2, HAS, conditions]


# Check for bunch/agg, pass into bunch_agg()   
# Else check for bunch or agg individually, pass into bunch() or agg_functions()
# Check for sort, pass into sort()
# Check for merge without sort, pass into sort_merge()
# Check for HAS, pass into has()
# Filter columns with get_columns()
# Locate most updated directory and print final table


def fetch(user_query_list):
    table = pd.read_pickle("./table/" + user_query_list[0] + ".pkl")
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    uqlupper = list(map(str.upper, user_query_list))
    if "MERGE" in uqlupper:
        table = merge(user_query_list)
    if "BUNCH" in uqlupper and (not set(agglist).isdisjoint(set(uqlupper))):
        table = bunch_agg(user_query_list, table)
    elif "BUNCH" in uqlupper:
        table = bunch(user_query_list, table)
    if "SORT" in uqlupper and "MERGE" not in uqlupper:
        table = sort(user_query_list)
    elif (not set(agglist).isdisjoint(set(uqlupper))):
        table = agg_functions(user_query_list, table)
        if type(table) == str:
            return
    if "HAS" in uqlupper:
        table = has(user_query_list)
    if "COLUMNS" in uqlupper:
        table = table.loc[:, get_columns(user_query_list)]
    print(table)

def get_columns(user_query_list): # Check to see if we're filtering columns correctly here
    uqlupper = list(map(str.upper, user_query_list))
    kwlist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX", "BUNCH", "SORT", "MERGE", "HAS"]
    idxlist = {}
    for kw in kwlist:
        if kw in uqlupper:
            idxlist[uqlupper.index(kw)] = kw
    if idxlist:
        nextkwidx = min(idxlist)
        cols_list = user_query_list[2:nextkwidx]
    else:
        cols_list = user_query_list[2:]
    agglist = kwlist[:5]
    if not set(agglist).isdisjoint(set(uqlupper)):
        if "SUM" in uqlupper:
            aggcol = user_query_list[uqlupper.index("SUM") + 1]
            aggcol = "sum_" + aggcol
            cols_list.append(aggcol)
        elif "TOTALNUM" in uqlupper:
            aggcol = user_query_list[uqlupper.index("TOTALNUM") + 1]
            aggcol = "totalnum_" + aggcol
            cols_list.append(aggcol)
        elif "MEAN" in uqlupper:
            aggcol = user_query_list[uqlupper.index("MEAN") + 1]
            aggcol = "mean_" + aggcol
            cols_list.append(aggcol)
        elif "MIN" in uqlupper:
            aggcol = user_query_list[uqlupper.index("MIN") + 1]
            aggcol = "min_" + aggcol
            cols_list.append(aggcol)
        elif "MAX" in uqlupper:
            aggcol = user_query_list[uqlupper.index("MAX") + 1]
            aggcol = "max_" + aggcol
            cols_list.append(aggcol)
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
    uqlupper = list(map(str.upper, user_query_list))
    col = user_query_list[uqlupper.index("SUM") + 1]
    if (not isinstance((table[col].iloc[0]), np.int64)) and (not isinstance((table[col].iloc[0]), np.float64)) and (not isinstance(table[col].iloc[0], int)) and (not isinstance(table[col].iloc[0], float)):
        print(type(table[col].iloc[0]))
        print("Error: SUM can only be used on a numeric column.")
        return "failed"
    if "COLUMNS" in uqlupper:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")   
            return 
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if "MERGE" in uqlupper or "HAS" in uqlupper:
                diruql = ("FETCH," + ",".join(user_query_list)).split(",")
                directory = find_directory(diruql)
            else:
                directory = chunk_path
            if not os.path.exists(chunk_path + "/col_agg"):
                os.mkdir(chunk_path + "/col_agg")
            total_sum = 0
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    total_sum += table[col].sum()
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    table.insert(len(table.columns), "sum_"+col, total_sum)
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_sum.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if "MERGE" in uqlupper or "HAS" in uqlupper:
            diruql = ("FETCH," + ",".join(user_query_list)).split(",")
            directory = find_directory(diruql)
        else:
            directory = chunk_path
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        total_sum = 0
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                total_sum += table[col].sum()
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                table.insert(len(table.columns), "sum_"+col, total_sum)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_sum.pkl")
    return table

def totalnum(user_query_list, table):
    uqlupper = list(map(str.upper, user_query_list))
    col = user_query_list[uqlupper.index("TOTALNUM") + 1]
    if "COLUMNS" in uqlupper:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")   
            return 
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if "MERGE" in uqlupper or "HAS" in uqlupper:
                diruql = ("FETCH," + ",".join(user_query_list)).split(",")
                directory = find_directory(diruql)
            else:
                directory = chunk_path
            if not os.path.exists(chunk_path + "/col_agg"):
                os.mkdir(chunk_path + "/col_agg")
            count = 0
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    count += len(table)
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    table.insert(len(table.columns), "totalnum_"+col, count)
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_totalnum.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if "MERGE" in uqlupper or "HAS" in uqlupper:
            diruql = ("FETCH," + ",".join(user_query_list)).split(",")
            directory = find_directory(diruql)
        else:
            directory = chunk_path
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        count = 0
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                count += len(table)
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                table.insert(len(table.columns), "totalnum_"+col, count)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_totalnum.pkl")
    return table

def mean(user_query_list, table):
    uqlupper = list(map(str.upper, user_query_list))
    col = user_query_list[uqlupper.index("MEAN") + 1]
    if (not isinstance((table[col].iloc[0]), int)) and (not isinstance((table[col].iloc[0]), float)):
        print("Error: MEAN can only be used on a numeric column.")
        return "failed"
    if "COLUMNS" in uqlupper:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")   
            return 
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if "MERGE" in uqlupper or "HAS" in uqlupper:
                diruql = ("FETCH," + ",".join(user_query_list)).split(",")
                directory = find_directory(diruql)
            else:
                directory = chunk_path
            if not os.path.exists(chunk_path + "/col_agg"):
                os.mkdir(chunk_path + "/col_agg")
            total_sum = 0
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    total_sum += table[col].sum()
            total_mean = total_sum/len(table)
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    table.insert(len(table.columns), "mean_"+col, total_mean)
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_mean.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if "MERGE" in uqlupper or "HAS" in uqlupper:
            diruql = ("FETCH," + ",".join(user_query_list)).split(",")
            directory = find_directory(diruql)
        else:
            directory = chunk_path
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        total_sum = 0
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                total_sum += table[col].sum()
        total_mean = total_sum/len(table)
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                table.insert(len(table.columns), "mean_"+col, total_mean)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_mean.pkl")
    return table

def tblmin(user_query_list, table):
    uqlupper = list(map(str.upper, user_query_list))
    col = user_query_list[uqlupper.index("MIN") + 1]
    if "COLUMNS" in uqlupper:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")   
            return 
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if "MERGE" in uqlupper or "HAS" in uqlupper:
                diruql = ("FETCH," + ",".join(user_query_list)).split(",")
                directory = find_directory(diruql)
            else:
                directory = chunk_path
            if not os.path.exists(chunk_path + "/col_agg"):
                os.mkdir(chunk_path + "/col_agg")
            min_val = float("inf")
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(chunk_path + "/" + chunk, index_col=0)
                    if table[col].min() < min_val:
                        min_val = table[col].min()
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    table.insert(len(table.columns), "min_"+col, min_val)
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_min.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if "MERGE" in uqlupper or "HAS" in uqlupper:
            diruql = ("FETCH," + ",".join(user_query_list)).split(",")
            directory = find_directory(diruql)
        else:
            directory = chunk_path
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        min_val = float("inf")
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk, index_col=0)
                if table[col].min() < min_val:
                    min_val = table[col].min()
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                table.insert(len(table.columns), "min_"+col, min_val)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_min.pkl")
    return table

def tblmax(user_query_list, table):
    uqlupper = list(map(str.upper, user_query_list))
    col = user_query_list[uqlupper.index("MAX") + 1]
    if "COLUMNS" in uqlupper:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")   
            return 
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if "MERGE" in uqlupper or "HAS" in uqlupper:
                diruql = ("FETCH," + ",".join(user_query_list)).split(",")
                directory = find_directory(diruql)
            else:
                directory = chunk_path
            if not os.path.exists(chunk_path + "/col_agg"):
                os.mkdir(chunk_path + "/col_agg")
            max_val = float("-inf")
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(chunk_path + "/" + chunk, index_col=0)
                    if table[col].min() > max_val:
                        max_val = table[col].max()
            for chunk in os.listdir(directory):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(directory + "/" + chunk, index_col=0)
                    table.insert(len(table.columns), "max_"+col, max_val)
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_max.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if "MERGE" in uqlupper or "HAS" in uqlupper:
            diruql = ("FETCH," + ",".join(user_query_list)).split(",")
            directory = find_directory(diruql)
        else:
            directory = chunk_path
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        max_val = float("-inf")
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk, index_col=0)
                if table[col].min() > max_val:
                    max_val = table[col].max()
        for chunk in os.listdir(directory):
            if chunk.endswith(".csv"):
                table = pd.read_csv(directory + "/" + chunk, index_col=0)
                table.insert(len(table.columns), "max_"+col, max_val)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_max.pkl")
    return table

def bunch_agg(user_query_list, table):
    agg_func = ""
    bunchidx = list(map(str.upper, user_query_list)).index("BUNCH") + 1
    bunchcol = user_query_list[bunchidx]
    if not os.path.exists("./" + user_query_list[0] + "_chunks"):
        print("Chunks folder does not exist")
        return
    else:
        bunch_agg_chunk_path=os.path.join("./" + user_query_list[0] + "_chunks", "bunch_agg_chunks")
        if not os.path.exists(bunch_agg_chunk_path):
            os.mkdir(bunch_agg_chunk_path)
    cols_list = get_columns(user_query_list)
    if bunchcol.upper() not in list(map(str.upper, cols_list)):
        print("Column to bunch must be selected in COLUMNS")
        return
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk, index_col=0)
                groups = table[bunchcol].unique()
                tbldict = {group: table[table[bunchcol] == group] for group in groups}
                if "SUM" in list(map(str.upper, user_query_list)):
                    agg_func = "sum"
                    sumcol = user_query_list[list(map(str.upper, user_query_list)).index("SUM") + 1]
                    if sumcol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    sumdict = {group: table.loc[table[bunchcol] == group, sumcol].sum() for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'sum_'+sumcol, sumdict[key])
                    final_table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                    final_table.to_pickle(bunch_agg_chunk_path + "/" + chunk[:-4] + "bunch_" + agg_func + ".pkl")
                    return final_table
                elif "TOTALNUM" in list(map(str.upper, user_query_list)):
                    agg_func = "totalnum"
                    totalcol = user_query_list[list(map(str.upper, user_query_list)).index("TOTALNUM") + 1]
                    if totalcol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    totaldict = {group: pd.Series(len(table.loc[table[bunchcol] == group, totalcol]), index=range(len(table[bunchcol])), name='totalnum') for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'totalnum_'+totalcol, totaldict[key])
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                    table.to_pickle(bunch_agg_chunk_path + "/" + chunk[:-4] + "bunch_" + agg_func + ".pkl")
                    return table
                elif "MEAN" in list(map(str.upper, user_query_list)):
                    agg_func = "mean"
                    meancol = user_query_list[list(map(str.upper, user_query_list)).index("MEAN") + 1]
                    if meancol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    meandict = {group: table.loc[table[bunchcol] == group, meancol].mean() for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'mean_'+meancol, meandict[key])
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                    table.to_pickle(bunch_agg_chunk_path + "/" + chunk[:-4] + "bunch_" + agg_func + ".pkl")
                    return table
                elif "MIN" in list(map(str.upper, user_query_list)):
                    agg_func = "min"
                    mincol = user_query_list[list(map(str.upper, user_query_list)).index("MIN") + 1]
                    if mincol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    mindict = {group: table.loc[table[bunchcol] == group, mincol].min() for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'min_'+mincol, mindict[key])
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                    table.to_pickle(bunch_agg_chunk_path + "/" + chunk[:-4] + "bunch_" + agg_func + ".pkl")
                    return table
                elif "MAX" in list(map(str.upper, user_query_list)):
                    agg_func = "max"
                    maxcol = user_query_list[list(map(str.upper, user_query_list)).index("MAX") + 1]
                    if maxcol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    maxdict = {group: table.loc[table[bunchcol] == group, maxcol].max() for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'max_'+maxcol, maxdict[key])
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                    table.to_pickle(bunch_agg_chunk_path + "/" + chunk[:-4] + "bunch_" + agg_func + ".pkl")
                    return table

def bunch(user_query_list, table):
    bunchidx = list(map(str.upper, user_query_list)).index("BUNCH") + 1
    bunchcol = user_query_list[bunchidx]
    if not os.path.exists("./" + user_query_list[0] + "_chunks"):
        print("Chunks folder does not exist")
        return
    else:
        if not os.path.exists("./" + user_query_list[0] + "_chunks/bunched_chunks"):
            os.mkdir("./" + user_query_list[0] + "_chunks/bunched_chunks")
        bunched_chunk_path=os.path.join("./" + user_query_list[0] + "_chunks", "bunched_chunks")
    chunk_path = "./" + user_query_list[0] + "_chunks"
    cols_list = get_columns(user_query_list)
    if bunchcol.upper() not in list(map(str.upper, cols_list)):
        print("Column to bunch must be selected in COLUMNS")
        return
    else:
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk, index_col=0)
                groups = table[bunchcol].unique()
                tbldict = {group: table[table[bunchcol] == group] for group in groups}
                table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                table.to_pickle(bunched_chunk_path + "/" + chunk[:-4] + "_bunch.pkl")
    return table

def merge(user_query_list):
    sortcol = user_query_list[list(map(str.upper, user_query_list)).index("INCOMMON") + 1]
    final_table = sort_merge(user_query_list, sortcol)
    return final_table

def has(user_query_list):
    uqlupper = list(map(str.upper, user_query_list))
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if "MERGE" in uqlupper:
       table = has_logic(user_query_list, "merged_tables")
    elif "SORT" in uqlupper:
        table = has_logic(user_query_list, "chunk_subsets")
    elif "BUNCH" in uqlupper:
        if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
            table = has_logic(user_query_list, "bunch_agg_chunks")
        else:
            table = has_logic(user_query_list, "bunched_chunks")
    elif not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
        if "COLUMNS" in uqlupper:
            table = has_logic(user_query_list, "col_agg")
        else:
            table = has_logic(user_query_list, "agg")
    else:
        table = has_logic(user_query_list, "")
    return table
    
def sort(user_query_list):
    sorted_df = pd.DataFrame()
    tablename = user_query_list[0]
    uqlupper = list(map(str.upper, user_query_list))
    sortcol = user_query_list[uqlupper.index("SORT") + 1]
    direction = None
    if "ASC" in uqlupper:
        direction = "ASC"
    elif "DESC" in uqlupper:
        direction = "DESC"
    elif direction is None:
        print("Please specify direction to sort by as ASC or DESC!")
        return
    if "BUNCH" in uqlupper:
        sorted_df = sort_bunch(user_query_list, sortcol)
    if "MERGE" in uqlupper:
        common_col = user_query_list[uqlupper.index("INCOMMON") + 1]
        sorted_df = sort_merge(user_query_list, common_col)
    elif "BUNCH" not in uqlupper and "MERGE" not in uqlupper:
        directory = sort_within_chunks(user_query_list, sortcol, "./" + tablename + "_chunks")
        sorted_df = sort_between_chunks(user_query_list, sortcol, directory)
    return sorted_df

def simple_sort(sortcol, table):
    table_list = table.values.tolist()
    tblcolumns = table.columns
    sortcol_index = tblcolumns.get_loc(sortcol)
    sorted_table = merge_sort(table_list, sortcol_index)
    sorted_table = pd.DataFrame(sorted_table, columns=tblcolumns)
    return sorted_table

def sort_bunch(user_query_list, sortcol):
    uqlupper = list(map(str.upper, user_query_list))
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    agg_present = not set(agglist).isdisjoint(set(uqlupper))
    which_agg = tuple(set(agglist).intersection(set(uqlupper)))[0]
    bunchcol = user_query_list[uqlupper.index("BUNCH") + 1] + "_bunched"
    if agg_present:
        file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunch_agg_chunks")
    else:
        file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunched_chunks")
    sorted_chunk_directory = "./"+ user_query_list[0] + "_chunks/sorted_chunks"
    for chunk in os.listdir(file_path):
        if os.path.isfile(file_path + "/" + chunk) and agg_present and chunk[0] != "." and chunk.endswith(which_agg.lower() + ".pkl"):
            table = pd.read_pickle(file_path + "/" + chunk)
            if bunchcol[:-8] == sortcol:
                sorted_chunk = table.sort_index(level=0)
                if not os.path.exists(sorted_chunk_directory):
                    os.mkdir(sorted_chunk_directory)
                sorted_chunk.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted_on_bunch.csv")
            else:
                indexes = table.index.get_level_values(0).unique().tolist()
                tbl_num = 1
                sortcol_index = table.columns.get_loc(sortcol)
                for i in indexes:
                    newtbl = table.loc[str(i), :]
                    newtbl = merge_sort(newtbl.values.tolist(), sortcol_index)
                    newtbl = pd.DataFrame(newtbl)
                    newtbl.columns = table.columns
                    newtbl.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted_level_" + str(tbl_num) + ".csv")
                    tbl_num += 1
        elif os.path.isfile(file_path + "/" + chunk) and not agg_present and chunk[0] != "." and chunk.endswith("_bunch.pkl"):
            table = pd.read_pickle(file_path + "/" + chunk)
            if bunchcol[:-8] == sortcol:
                sorted_chunk = table.sort_index(level=0)
                if not os.path.exists(sorted_chunk_directory):
                    os.mkdir(sorted_chunk_directory)
                sorted_chunk.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted_on_bunch.csv")
            else:
                indexes = table.index.get_level_values(0).unique().tolist()
                tbl_num = 1
                sortcol_index = table.columns.get_loc(sortcol)
                for i in indexes:
                    newtbl = table.loc[str(i), :]
                    newtbl = merge_sort(newtbl.values.tolist(), sortcol_index)
                    newtbl = pd.DataFrame(newtbl)
                    newtbl.columns = table.columns
                    newtbl.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted_level_" + str(tbl_num) + ".csv")
                    tbl_num += 1
    dflist = []
    for filename in sorted(os.listdir(sorted_chunk_directory)):
        if bunchcol[:-8] != sortcol:
            if "level" in filename:
                sorted_chunk = pd.read_csv(sorted_chunk_directory + "/" + filename, index_col=0)
                dflist.append(sorted_chunk)
        else:
            if ("sorted_on_bunch" in filename) and (which_agg.lower() in filename):
                sorted_chunk = pd.read_csv(sorted_chunk_directory + "/" + filename, index_col=0)
                dflist.append(sorted_chunk)
    final_sorted_table = pd.concat(dflist, ignore_index=True)
    groups = final_sorted_table[bunchcol[:-8]].unique()
    tbldict = {group: final_sorted_table[final_sorted_table[bunchcol[:-8]] == group] for group in groups}
    final_sorted_table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol, 'ROWID'])
    return final_sorted_table

def sort_merge(user_query_list, sortcol):
    tbl1 = user_query_list[0]
    tbl2 = user_query_list[list(map(str.upper, user_query_list)).index("MERGE") + 1]
    directory1 = "./" + tbl1 + "_chunks"
    directory2 = "./" + tbl2 + "_chunks"
    if not os.path.exists(directory1 + "/merged_tables"):
        os.mkdir(directory1 + "/merged_tables")
    merged_directory1 = directory1 + "/merged_tables"
    if not os.path.exists(directory2 + "/merged_tables"):
        os.mkdir(directory2 + "/merged_tables")
    merged_directory2 = directory2 + "/merged_tables"
    for filename in os.listdir(directory1):
        if os.path.isfile(directory1 + "/" + filename) and filename[0] != ".":
            newtbl = pd.read_csv(directory1 + "/" + filename, index_col=0)
            newtbl = simple_sort(sortcol, newtbl)
            newtbl.to_csv(merged_directory1 + "/" + filename)
    for filename in os.listdir(directory2):
        if os.path.isfile(directory2 + "/" + filename) and filename[0] != ".":
            newtbl2 = pd.read_csv(directory2 + "/" + filename, index_col=0)
            newtbl2 = simple_sort(sortcol, newtbl2)
            newtbl2.to_csv(merged_directory2 + "/" + filename)
    df_list = []
    for left_chunk in os.listdir(merged_directory1):
        for right_chunk in os.listdir(merged_directory2):
            if os.path.isfile(merged_directory1 + "/" + left_chunk) and os.path.isfile(merged_directory2 + "/" + right_chunk) and left_chunk[0] != "." and right_chunk[0] != ".":
                if "merged" not in left_chunk and "merged" not in right_chunk:
                    left = pd.read_csv(directory1 + "/" + left_chunk, index_col=0)
                    right = pd.read_csv(directory2 + "/" + right_chunk, index_col=0)
                    sortcol_type = left[sortcol].dtype
                    for i1 in range(len(left)):
                        t1 = left.iloc[i1]
                        if t1.isnull().any():
                            continue
                        t1 = pd.DataFrame(t1, index=left.columns).T
                        t1.reset_index(drop=True, inplace=True)
                        matchval = t1[sortcol].values
                        for i2 in range(len(right)):
                            t2 = right.iloc[i2]
                            if t2.isnull().any():
                                continue
                            t2 = pd.DataFrame(t2, index=right.columns).T
                            t2.reset_index(drop=True, inplace=True)
                            if t1[sortcol].values == t2[sortcol].values:
                                concat_tbl = pd.concat([t1, t2], axis=1)
                                concat_tbl = concat_tbl.loc[:, ~concat_tbl.columns.duplicated()]
                                df_list.append(concat_tbl)
                                right.replace(matchval, np.nan, inplace=True)
                                break
                        left.replace(matchval, np.nan, inplace=True)
    final_merged_table = pd.concat(df_list)
    final_merged_table[sortcol] = final_merged_table[sortcol].astype(sortcol_type)
    for fn in os.listdir(merged_directory1):
        if os.path.isfile(merged_directory1 + "/" + fn):
            os.remove(merged_directory1 + "/" + fn)
    for fn in os.listdir(merged_directory2):
        if os.path.isfile(merged_directory2 + "/" + fn):
            os.remove(merged_directory2 + "/" + fn)
    final_merged_table.to_csv(merged_directory1 + "/" + tbl1 + "_merged.csv")
    final_merged_table.to_csv(merged_directory2 + "/" + tbl2 + "_merged.csv")
    return final_merged_table

def sort_within_chunks(user_query_list, sortcol, directory):
    tablename = user_query_list[0]
    uqlupper = list(map(str.upper, user_query_list))
    if "ASC" in uqlupper:
        asc = True
    elif "DESC" in uqlupper:
        asc = False
    table = pd.DataFrame()
    sorted_dir = "./" + tablename + "_chunks/sorted_chunks"
    if not os.path.exists(sorted_dir):
        os.mkdir(sorted_dir)
    for filename in os.listdir(directory):
        if os.path.isfile(directory + "/" + filename) and filename[0] != ".":
            if filename.endswith(".csv"):
                table = pd.read_csv(os.path.join(directory, filename), index_col=0)
            else:
                table = pd.read_pickle(os.path.join(directory, filename))
            table.sort_values(sortcol, ascending=asc, inplace=True)
            table.to_csv(sorted_dir + "/" + filename[:-4] + "_sorted.csv")
    return sorted_dir

def sort_between_chunks(user_query_list, sortcol, directory):
    tablename = user_query_list[0]
    if not os.path.exists("./" + tablename + "_chunks/chunk_subsets"):
        os.mkdir("./" + tablename + "_chunks/chunk_subsets")
    subset_count = 1
    for filename in os.listdir(directory):
        if os.path.isfile(directory + "/" + filename) and filename[0] != ".":
            file_to_subset = pd.read_csv(directory + "/" + filename, index_col=0)
            sortcol_idx = file_to_subset.columns.get_loc(sortcol)
            head = list(file_to_subset.columns)
            for skiprownum in range(0, len(file_to_subset), 450):
                if skiprownum == 0:
                    file_subset = pd.read_csv(directory + "/" + filename, names=head, nrows=450, skiprows=1, index_col=0)
                else:
                    file_subset = pd.read_csv(directory + "/" + filename, names=head, nrows=450, skiprows=skiprownum, index_col=0)
                file_subset_name = f"{filename.split('.')[0]}_subset{subset_count}.pkl"
                file_subset_path = "./" + tablename + "_chunks/chunk_subsets/" + file_subset_name
                file_subset.to_pickle(file_subset_path)
                subset_count += 1
                if len(file_subset) == 100:
                    break
    subset_files = os.listdir("./"+ tablename + "_chunks/chunk_subsets")
    while len(subset_files) > 1:
        print('iterating through subsets to merge')
        chunk1 = pd.read_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", subset_files.pop(0))) #get the first file 
        chunk2 = pd.read_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", subset_files.pop(0))) #second file 
        for col in list(chunk1.columns):
            chunk1[col] = chunk1[col].astype(pd.Series(chunk1.loc[1, col]).dtype)
            chunk2[col] = chunk2[col].astype(pd.Series(chunk2.loc[1, col]).dtype)
        pd.DataFrame(merge_asc(chunk1.values.tolist(), chunk2.values.tolist(), sortcol_idx)).to_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", f"merged_subset_{len(subset_files) + 1 }.pkl"))
        subset_files.append(f"merged_subset_{len(subset_files) + 1 }.pkl")
    final_merged_table = pd.read_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", subset_files[0]))
    for fn in os.listdir("./"+ tablename + "_chunks/chunk_subsets"):
        os.remove("./"+ tablename + "_chunks/chunk_subsets/" + fn)
    final_merged_table.columns = head
    return final_merged_table

def merge_sort(table_list, sortcol_index): 
    if len(table_list) == 1:
        return table_list
    mid = len(table_list)//2
    left = merge_sort(table_list[:mid], sortcol_index)
    right = merge_sort(table_list[mid:], sortcol_index)
    sorted_table = merge_asc(left, right, sortcol_index)
    return sorted_table

def merge_asc(left, right, sortcol_index): 
    sorted_table = [] 
    i = 0 
    j = 0 
    while i < len(left) and j < len(right):
        if left[i][sortcol_index] < right[j][sortcol_index]:
            sorted_table.append(left[i])
            i = i + 1 
        else: 
            sorted_table.append(right[j])
            j = j + 1 
    while i < len(left): 
        sorted_table.append(left[i])
        i = i + 1 
    while j < len(right): 
        sorted_table.append(right[j])
        j = j + 1
    return sorted_table

def has_logic(user_query_list, directory):
    uqlupper = list(map(str.upper, user_query_list))
    condidx = uqlupper.index("HAS") + 1
    full_condition = ""
    if condidx != len(uqlupper) - 1:
        conds = user_query_list[condidx:]
        for c in conds:
            full_condition += " "
            full_condition += c
    else:
        full_condition = user_query_list[condidx]
    condition = full_condition.strip().replace("\"", "").replace("\'", "")
    for chunk in os.listdir("./" + user_query_list[0] + "_chunks/" + directory):
        if os.path.isfile("./" + user_query_list[0] + "_chunks/" + directory + "/" + chunk) and chunk[0] != ".":
            if "MERGE" in uqlupper and "merged" in chunk:
                table = pd.read_csv("./" + user_query_list[0] + "_chunks/" + directory + "/" + chunk, index_col=0)
            elif "MERGE" not in uqlupper:
                table = pd.read_csv("./" + user_query_list[0] + "_chunks/" + directory + "/" + chunk, index_col=0)
            if "COLUMNS" in uqlupper:
                table = table.loc[:, get_columns(user_query_list)]
            if "<" in condition:
                cond = condition.split("<")
                col1 = cond[0]
                cond2 = cond[1]
                if col1 not in table.columns:
                    return 
                if cond2 not in table.columns:
                    type1 = type(table[col1].iloc[0])
                    if '.' in cond2:
                        cond2 = float(cond2)
                    elif cond2.isdigit():
                        cond2 = int(cond2)
                    type2 = type(cond2)
                    if type1 == type2: 
                        table = table.loc[table[col1] < cond2]
                    else:   
                        if isinstance(type1, str) or isinstance(type2, str) or isinstance(type1, pd.DatetimeIndex) or isinstance(type2, pd.DatetimeIndex):
                            print("Incompatible type comparison")
                            return
                        else:
                            table = table.loc[table[col1] < cond2]
                else:
                    table = table.loc[table[col1] < table[cond2]]
            elif ">" in condition:
                cond = condition.split(">")
                col1 = cond[0]
                cond2 = cond[1]
                if col1 not in table.columns:
                    return 
                if cond2 not in table.columns:
                    type1 = type(table[col1].iloc[0])
                    if '.' in cond2:
                        cond2 = float(cond2)
                    elif cond2.isdigit():
                        cond2 = int(cond2)
                    type2 = type(cond2)
                    if type1 == type2: 
                        table = table.loc[table[col1] > cond2]
                    else:   
                        if isinstance(type1, str) or isinstance(type2, str) or isinstance(type1, pd.DatetimeIndex) or isinstance(type2, pd.DatetimeIndex):
                            print("Incompatible type comparison")
                            return
                        else:
                            table = table.loc[table[col1] > cond2]
                else:
                    table = table.loc[table[col1] > table[cond2]]
            elif "=" in condition:
                cond = condition.split("=")
                col1 = cond[0]
                cond2 = cond[1]
                if col1 not in table.columns:
                    return 
                if cond2 not in table.columns:
                    type1 = type(table[col1].iloc[0])
                    if '.' in cond2:
                        cond2 = float(cond2)
                    elif cond2.isdigit():
                        cond2 = int(cond2)
                    type2 = type(cond2)
                    if type1 == type2: 
                        table = table.loc[table[col1] == cond2]
                    else:   
                        if isinstance(type1, str) or isinstance(type2, str) or isinstance(type1, pd.DatetimeIndex) or isinstance(type2, pd.DatetimeIndex):
                            print("Incompatible type comparison")
                            return
                        else:
                            table = table.loc[table[col1] == cond2]
                else:
                    table = table.loc[table[col1] == table[cond2]]
        else:
            continue
        if not os.path.exists("./" + user_query_list[0] + "_chunks/" + directory + "/has_chunks"):
            os.mkdir("./" + user_query_list[0] + "_chunks/" + directory + "/has_chunks")
        table.to_pickle("./" + user_query_list[0] + "_chunks/" + directory + "/has_chunks/" + user_query_list[0] + "_has.pkl")
        return table