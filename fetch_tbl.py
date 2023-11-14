import pandas as pd
import os
import warnings
import sort
from printoutput import return_table
warnings.simplefilter(action='ignore', category=FutureWarning)

# [tablename, COLUMNS, columns, TOTALNUM/SUM/MEAN/MIN/MAX, column, 
# BUNCH, columns, SORT, columns, ASC/DESC, MERGE, tablename2, HAS, conditions]

def fetch(user_query_list):
    table = pd.read_pickle(user_query_list[0] + ".pkl")
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if "BUNCH" in user_query_list and (not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list))))):
        table = bunch_agg(user_query_list, table)
    elif "BUNCH" in user_query_list:
        table = bunch(user_query_list, table)
    elif (not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list))))):
        table = agg_functions(user_query_list, table)
    if "SORT" in user_query_list:
        table = sort.sort(user_query_list)
    if "MERGE" in user_query_list:
        mergetbl = user_query_list[list(map(str.upper, user_query_list)).index("MERGE") + 1]
        table = merge(user_query_list, table, mergetbl)
    if "HAS" in user_query_list:
        table = has(user_query_list)
    return return_table(user_query_list)

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
    col = user_query_list[list(map(str.upper, user_query_list)).index("SUM") + 1]
    if (table.dtypes[col] != pd.Int64Dtype) and (table.dtypes[col] != pd.Float64Dtype):
        print("SUM can only be used on a numeric column")
        return
    if "COLUMNS" in user_query_list:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")   
            return 
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if not os.path.exists(chunk_path + "/col_agg"):
                os.mkdir(chunk_path + "/col_agg")
            for chunk in os.listdir(chunk_path):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                    sumseries = pd.Series(table[col].sum(), index=range(len(table[col])))
                    table.insert(len(table.columns), "sum_"+col, sumseries)
                    table.to_pickle(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk" + chunk + "_col_sum.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                sumseries = pd.Series(table[col].sum(), index=range(len(table[col])))
                table.insert(len(table.columns), "sum_"+col, sumseries)
                table.to_pickle(chunk_path + "/agg/" + user_query_list[0] + "_chunk" + chunk + "_sum.pkl")
        return table

def totalnum(user_query_list, table):
    col = user_query_list[list(map(str.upper, user_query_list)).index("TOTALNUM") + 1]
    if (table.dtypes[col] != pd.Int64Dtype) and (table.dtypes[col] != pd.Float64Dtype):
        print("TOTALNUM can only be used on a numeric column")
        return
    if "COLUMNS" in user_query_list:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")  
            return  
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if not os.path.exists("./" + user_query_list[0] + "_chunks/col_agg"):
                os.mkdir("./" + user_query_list[0] + "_chunks/col_agg")
            for chunk in os.listdir(chunk_path):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                    totalseries = pd.Series(len(table[col]), index=range(len(table[col])))
                    table.insert(len(table.columns), "totalnum_"+col, totalseries)
                    table.to_pickle(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk" + chunk + "_col_totalnum.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                totalseries = pd.Series(len(table[col]), index=range(len(table[col])))
                table.insert(len(table.columns), "totalnum_"+col, totalseries)
                table.to_pickle(chunk_path + "/agg/" + user_query_list[0] + "_chunk" + chunk + "_totalnum.pkl")
    return table

def mean(user_query_list, table):
    col = user_query_list[list(map(str.upper, user_query_list)).index("MEAN") + 1]
    if (table.dtypes[col] != pd.Int64Dtype) and (table.dtypes[col] != pd.Float64Dtype):
        print("MEAN can only be used on a numeric column")
        return
    if "COLUMNS" in user_query_list:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")
            return    
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if not os.path.exists("./" + user_query_list[0] + "_chunks/col_agg"):
                os.mkdir("./" + user_query_list[0] + "_chunks/col_agg")
            for chunk in os.listdir(chunk_path):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                    meanseries = pd.Series(table[col].mean(), index=range(len(table[col])))
                    table.insert(len(table.columns), "mean_"+col, meanseries)
                    table.to_pickle(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk" + chunk + "_col_mean.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                meanseries = pd.Series(table[col].mean(), index=range(len(table[col])))
                table.insert(len(table.columns), "mean_"+col, meanseries)
                table.to_pickle(chunk_path + "/agg/" + user_query_list[0] + "_chunk" + chunk + "_mean.pkl")
        return table

def tblmin(user_query_list, table):
    col = user_query_list[list(map(str.upper, user_query_list)).index("MIN") + 1]
    if "COLUMNS" in user_query_list:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")    
            return
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if not os.path.exists("./" + user_query_list[0] + "_chunks/col_agg"):
                os.mkdir("./" + user_query_list[0] + "_chunks/col_agg")
            for chunk in os.listdir(chunk_path):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                    minseries = pd.Series(table[col].min(), index=range(len(table[col])))
                    table.insert(len(table.columns), "min_"+col, minseries)
                    table.to_pickle(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk" + chunk + "_col_min.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                minseries = pd.Series(table[col].min(), index=range(len(table[col])))
                table.insert(len(table.columns), "min_"+col, minseries)
                table.to_pickle(chunk_path + "/agg/" + user_query_list[0] + "_chunk" + chunk + "_min.pkl")
    return table

def tblmax(user_query_list, table):
    col = user_query_list[list(map(str.upper, user_query_list)).index("MAX") + 1]
    if "COLUMNS" in user_query_list:
        cols_list = get_columns(user_query_list)
        if col.upper() not in list(map(str.upper, cols_list)):
            print("Column to aggregate must be selected in COLUMNS")    
            return
        else:
            chunk_path = "./" + user_query_list[0] + "_chunks"
            if not os.path.exists("./" + user_query_list[0] + "_chunks/col_agg"):
                os.mkdir("./" + user_query_list[0] + "_chunks/col_agg")
            for chunk in os.listdir(chunk_path):
                if chunk.endswith(".csv"):
                    table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                    maxseries = pd.Series(table[col].max(), index=range(len(table[col])))
                    table.insert(len(table.columns), "max_"+col, maxseries)
                    table.to_pickle(chunk_path + "/col_agg/" + user_query_list[0] + "_chunk" + chunk + "_col_max.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                maxseries = pd.Series(table[col].max(), index=range(len(table[col])))
                table.insert(len(table.columns), "max_"+col, maxseries)
                table.to_pickle(chunk_path + "/agg/" + user_query_list[0] + "_chunk" + chunk + "_max.pkl")
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
                table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                groups = table[bunchcol].unique()
                tbldict = {group: table[table[bunchcol] == group] for group in groups}
                if "SUM" in user_query_list:
                    agg_func = "sum"
                    sumcol = user_query_list[list(map(str.upper, user_query_list)).index("SUM") + 1]
                    if sumcol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    sumdict = {group: pd.Series(table.loc[table[bunchcol] == group, sumcol].sum(), index=range(len(table[bunchcol])), name='sum') for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'sum_'+sumcol, sumdict[key])
                    table = pd.concat([v for v in tbldict.values()])
                    print(table.head())
                elif "TOTALNUM" in user_query_list:
                    agg_func = "totalnum"
                    totalcol = user_query_list[list(map(str.upper, user_query_list)).index("TOTALNUM") + 1]
                    if totalcol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    totaldict = {group: pd.Series(len(table.loc[table[bunchcol] == group, totalcol]), index=range(len(table[bunchcol])), name='totalnum') for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'totalnum_'+totalcol, totaldict[key])
                    table = pd.concat([v for v in tbldict.values()])
                    print(table.head())
                elif "MEAN" in user_query_list:
                    agg_func = "mean"
                    meancol = user_query_list[list(map(str.upper, user_query_list)).index("MEAN") + 1]
                    if meancol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    meandict = {group: pd.Series(table.loc[table[bunchcol] == group, meancol].mean(), index=range(len(table[bunchcol])), name='mean') for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'mean_'+meancol, meandict[key])
                    table = pd.concat([v for v in tbldict.values()])
                    print(table.head())
                elif "MIN" in user_query_list:
                    agg_func = "min"
                    mincol = user_query_list[list(map(str.upper, user_query_list)).index("MIN") + 1]
                    if mincol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    mindict = {group: pd.Series(table.loc[table[bunchcol] == group, mincol].min(), index=range(len(table[bunchcol])), name='min') for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'min_'+mincol, mindict[key])
                    table = pd.concat([v for v in tbldict.values()])
                    print(table.head())
                elif "MAX" in user_query_list:
                    agg_func = "max"
                    maxcol = user_query_list[list(map(str.upper, user_query_list)).index("MAX") + 1]
                    if maxcol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    maxdict = {group: pd.Series(table.loc[table[bunchcol] == group, maxcol].max(), index=range(len(table[bunchcol])), name='max') for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'max_'+maxcol, maxdict[key])
                    table = pd.concat([v for v in tbldict.values()])
                    print(table.head())
                table.to_pickle(bunch_agg_chunk_path + "/" + user_query_list[0] + "_chunk" + chunk[:-4] + "bunch_" + agg_func + ".pkl")
        print("grouped successfully by", bunchcol, "; aggregate function is", agg_func)
    return table

def bunch(user_query_list, table):
    bunchidx = list(map(str.upper, user_query_list)).index("BUNCH") + 1
    bunchcol = user_query_list[bunchidx]
    if not os.path.exists("./" + user_query_list[0] + "_chunks"):
        print("Chunks folder does not exist")
        return
    else:
        bunched_chunk_path=os.path.join("./" + user_query_list[0] + "_chunks", "bunched_chunks")
    chunk_path = "./" + user_query_list[0] + "_chunks"
    cols_list = get_columns(user_query_list)
    if bunchcol.upper() not in list(map(str.upper, cols_list)):
        print("Column to bunch must be selected in COLUMNS")
        return
    else:
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                groups = table[bunchcol].unique()
                tbldict = {group: table[table[bunchcol] == group] for group in groups}
                table = pd.concat([v for v in tbldict.values()])
                table.to_pickle(bunched_chunk_path + "/" + user_query_list[0] + "_chunk" + chunk[:-4] + "bunch.pkl")
        print("grouped successfully by", bunchcol)
    return table

def merge(user_query_list, table1, table2):
    if "INCOMMON" not in user_query_list:
        print("Must provide column to merge on. Syntax: MERGE table1 table2 INCOMMON column")
        return
    col = user_query_list[list(map(str.upper, user_query_list)).index("INCOMMON") + 1]
    tblname = user_query_list[0]
    mergetblname = user_query_list[list(map(str.upper, user_query_list)).index("MERGE") + 1]
    table1 = sort.sort(table1)
    table2 = sort.sort(table2)
    final_table = sort.merge_asc(table1.tolist(), table2.tolist(), col)
    if not os.path.exists("./" + user_query_list[0] + "_chunks/merged_tables"):
        os.mkdir("./" + user_query_list[0] + "_chunks/merged_tables")
    final_table.to_pickle("./" + user_query_list[0] + "_chunks/merged_tables/merged_" + tblname + "_" + mergetblname + ".pkl")
    return final_table

def has(user_query_list):
    user_query_list = list(map(str.upper, user_query_list))
    condidx = user_query_list.index("HAS") + 1
    cond = user_query_list[condidx]
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if "MERGE" in user_query_list: 
        table = pd.read_pickle("./" + user_query_list[0] + "_chunks/merged_tables/" + os.listdir("./" + user_query_list[0] + "_chunks/merged_tables")[0])
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
        if not os.path.exists("./" + user_query_list[0] + "_chunks/merged_tables/has_chunks"):
            os.mkdir("./" + user_query_list[0] + "_chunks/merged_tables/has_chunks")
        table.to_pickle("./" + user_query_list[0] + "_chunks/merged_tables/has_chunks/" + user_query_list[0] + "_has.pkl")
    elif "SORT" in user_query_list:
        table = pd.read_pickle("./" + user_query_list[0] + "_chunks/chunk_subsets/" + os.listdir("./" + user_query_list[0] + "_chunks/chunk_subsets")[0])
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
        if not os.path.exists("./" + user_query_list[0] + "_chunks/chunk_subsets/has_chunks"):
            os.mkdir("./" + user_query_list[0] + "_chunks/chunk_subsets/has_chunks")
        table.to_pickle("./" + user_query_list[0] + "_chunks/chunk_subsets/has_chunks/" + user_query_list[0] + "_has.pkl")
    elif "BUNCH" in user_query_list:
        if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
            for chunk in os.listdir("./" + user_query_list[0] + "_chunks/bunch_agg_chunks"):
                table = pd.read_pickle(chunk)
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
                if not os.path.exists("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/has_chunks/" + user_query_list[0] + "_chunk" + chunk + "_has.pkl")
        else:
            for chunk in os.listdir("./" + user_query_list[0] + "_chunks/bunched_chunks"):
                table = pd.read_pickle(chunk)
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
                if not os.path.exists("./" + user_query_list[0] + "_chunks/bunched_chunks/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/bunched_chunks/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/bunched_chunks/has_chunks/" + user_query_list[0] + "_chunk" + chunk + "_has.pkl")
    elif not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
        if "COLUMNS" in user_query_list:
            for chunk in os.listdir("./" + user_query_list[0] + "_chunks/col_agg"):
                table = pd.read_pickle(chunk)
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
                if not os.path.exists("./" + user_query_list[0] + "_chunks/col_agg/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/col_agg/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/col_agg/has_chunks/" + user_query_list[0] + "_chunk" + chunk + "_has.pkl")
        else:
            for chunk in os.listdir("./" + user_query_list[0] + "_chunks/agg"):
                table = pd.read_pickle(chunk)
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
                if not os.path.exists("./" + user_query_list[0] + "_chunks/agg/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/agg/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/col_agg/has_chunks/" + user_query_list[0] + "_chunk" + chunk + "_has.pkl")
    elif "COLUMNS" in user_query_list:
        for chunk in os.listdir("./" + user_query_list[0] + "_chunks"):
            table = pd.read_csv(chunk).loc[:, get_columns(user_query_list)]
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
            if not os.path.exists("./" + user_query_list[0] + "_chunks/has_chunks"):
                os.mkdir("./" + user_query_list[0] + "_chunks/has_chunks")
            table.to_pickle("./" + user_query_list[0] + "_chunks/has_chunks" + user_query_list[0] + "_chunk" + chunk + "_has.pkl")
    return table