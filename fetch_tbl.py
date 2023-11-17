import pandas as pd
import os
import warnings
from printoutput import return_table, find_directory
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
    if "BUNCH" in uqlupper and (not set(agglist).isdisjoint(set(uqlupper))):
        table = bunch_agg(user_query_list, table)
    elif "BUNCH" in uqlupper:
        table = bunch(user_query_list, table)
    elif (not set(agglist).isdisjoint(set(uqlupper))):
        table = agg_functions(user_query_list, table)
        if table == "failed":
            return
    if "SORT" in uqlupper:
        print("In sort function")
        table = sort(user_query_list)
    elif "MERGE" in uqlupper:
        table = merge(user_query_list)
    if "HAS" in uqlupper:
        table = has(user_query_list)
    if "COLUMNS" in uqlupper:
        table = table[get_columns(user_query_list)]
    user_query_list.insert(0, "FETCH")
    if "BUNCH" in uqlupper and "SORT" in uqlupper:
        print("bunched and sorted table: \n", table)
    elif (not set(agglist).isdisjoint(set(uqlupper))) or ("BUNCH" in uqlupper) or ("MERGE" in uqlupper) or ("SORT" in uqlupper) or ("HAS" in uqlupper):
        if (not set(agglist).isdisjoint(set(uqlupper))) and ("BUNCH" in uqlupper):
            agg_func = set(agglist).intersection(set(uqlupper)).pop()
            print(return_table(user_query_list, user_query_list[1], agg_function=agg_func))
        else: 
            print(return_table(user_query_list, user_query_list[1]))
    else:
        print(table)

def get_columns(user_query_list):
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
    if (not isinstance((table[col].iloc[0]), int)) and (not isinstance((table[col].iloc[0]), float)):
        print("Error: SUM can only be used on a numeric column.")
        return "failed"
    if "COLUMNS" in uqlupper:
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
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_sum.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                sumseries = pd.Series(table[col].sum(), index=range(len(table[col])))
                table.insert(len(table.columns), "sum_"+col, sumseries)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_sum.pkl")
        return table

def totalnum(user_query_list, table):
    col = user_query_list[list(map(str.upper, user_query_list)).index("TOTALNUM") + 1]
    if (not isinstance((table[col].iloc[0]), int)) and (not isinstance((table[col].iloc[0]), float)):
        print("Error: TOTALNUM can only be used on a numeric column.")
        return "failed"
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
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_totalnum.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                totalseries = pd.Series(len(table[col]), index=range(len(table[col])))
                table.insert(len(table.columns), "totalnum_"+col, totalseries)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_totalnum.pkl")
    return table

def mean(user_query_list, table):
    col = user_query_list[list(map(str.upper, user_query_list)).index("MEAN") + 1]
    if (not isinstance((table[col].iloc[0]), int)) and (not isinstance((table[col].iloc[0]), float)):
        print("Error: MEAN can only be used on a numeric column.")
        return "failed"
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
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_mean.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                meanseries = pd.Series(table[col].mean(), index=range(len(table[col])))
                table.insert(len(table.columns), "mean_"+col, meanseries)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_mean.pkl")
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
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_min.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                minseries = pd.Series(table[col].min(), index=range(len(table[col])))
                table.insert(len(table.columns), "min_"+col, minseries)
                table.to_pickle(chunk_path + "/agg/" + chunk[:-4] + "_min.pkl")
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
                    table.to_pickle(chunk_path + "/col_agg/" + chunk[:-4] + "_col_max.pkl")
    else:
        chunk_path = "./" + user_query_list[0] + "_chunks"
        if not os.path.exists("./" + user_query_list[0] + "_chunks/agg"):
            os.mkdir("./" + user_query_list[0] + "_chunks/agg")
        for chunk in os.listdir(chunk_path):
            if chunk.endswith(".csv"):
                table = pd.read_csv(chunk_path + "/" + chunk)
                maxseries = pd.Series(table[col].max(), index=range(len(table[col])))
                table.insert(len(table.columns), "max_"+col, maxseries)
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
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                    print(table.head())
                elif "TOTALNUM" in user_query_list:
                    print("Table before doing TOTAL NUM",table)
                    agg_func = "totalnum"
                    totalcol = user_query_list[list(map(str.upper, user_query_list)).index("TOTALNUM") + 1]
                    if totalcol.upper() not in list(map(str.upper, cols_list)):
                            print("Column to aggregate must be selected in COLUMNS")
                            return
                    totaldict = {group: pd.Series(len(table.loc[table[bunchcol] == group, totalcol]), index=range(len(table[bunchcol])), name='totalnum') for group in groups}
                    for key in tbldict.keys():
                        tbldict[key].insert(len(tbldict[key].columns), 'totalnum_'+totalcol, totaldict[key])
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
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
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
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
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
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
                    table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                    #print(table.head())
                print("Table dict", tbldict)
                table.to_pickle(bunch_agg_chunk_path + "/" + chunk[:-4] + "bunch_" + agg_func + ".pkl")
        print("grouped successfully by", bunchcol, "; aggregate function is", agg_func)
        
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
                table = pd.read_csv(chunk_path + "/" + chunk).loc[:, cols_list]
                groups = table[bunchcol].unique()
                tbldict = {group: table[table[bunchcol] == group] for group in groups}
                table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol + '_bunched', 'ROWID'])
                table.to_pickle(bunched_chunk_path + "/" + chunk[:-4] + "_bunch.pkl")
        print("grouped successfully by", bunchcol)
    return table

def merge(user_query_list):
    sortcol = user_query_list[list(map(str.upper, user_query_list)).index("INCOMMON") + 1]
    final_table = sort_merge(user_query_list, sortcol)
    return final_table

def has(user_query_list):
    uqlupper = list(map(str.upper, user_query_list))
    condidx = uqlupper.index("HAS") + 1
    cond = user_query_list[condidx]
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if "MERGE" in uqlupper: 
       # table = pd.read_pickle("./" + user_query_list[0] + "_chunks/merged_tables/" + os.listdir("./" + user_query_list[0] + "_chunks/merged_tables")[0])
       for chunk in os.listdir("./" + user_query_list[0] + "_chunks/merged_tables"):
            if os.path.isfile("./" + user_query_list[0] + "_chunks/merged_tables/" + chunk):
                table = pd.read_csv("./" + user_query_list[0] + "_chunks/merged_tables/" + chunk).loc[:, get_columns(user_query_list)]
                if "<" in cond:
                    cond = user_query_list[condidx].split("<")
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
                elif ">" in cond:
                    cond = user_query_list[condidx].split(">")
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
                elif "=" in cond:
                    cond = user_query_list[condidx].split("=")
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
            if not os.path.exists("./" + user_query_list[0] + "_chunks/merged_tables/has_chunks"):
                os.mkdir("./" + user_query_list[0] + "_chunks/merged_tables/has_chunks")
            table.to_pickle("./" + user_query_list[0] + "_chunks/merged_tables/has_chunks/" + user_query_list[0] + "_has.pkl")
    elif "SORT" in uqlupper:
        #table = pd.read_pickle("./" + user_query_list[0] + "_chunks/chunk_subsets/" + os.listdir("./" + user_query_list[0] + "_chunks/chunk_subsets")[0])
        for chunk in os.listdir("./" + user_query_list[0] + "_chunks/chunk_subsets"):
            if os.path.isfile("./" + user_query_list[0] + "_chunks/chunk_subsets/" + chunk):
                table = pd.read_csv("./" + user_query_list[0] + "_chunks/chunk_subsets/" + chunk).loc[:, get_columns(user_query_list)]
                if "<" in cond:
                    cond = user_query_list[condidx].split("<")
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
                elif ">" in cond:
                    cond = user_query_list[condidx].split(">")
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
                elif "=" in cond:
                    cond = user_query_list[condidx].split("=")
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
            if not os.path.exists("./" + user_query_list[0] + "_chunks/chunk_subsets/has_chunks"):
                os.mkdir("./" + user_query_list[0] + "_chunks/chunk_subsets/has_chunks")
            table.to_pickle("./" + user_query_list[0] + "_chunks/chunk_subsets/has_chunks/" + user_query_list[0] + "_has.pkl")
    elif "BUNCH" in uqlupper:
        if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
             for chunk in os.listdir("./" + user_query_list[0] + "_chunks/bunch_agg_chunks"):
                if os.path.isfile("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/" + chunk):
                    table = pd.read_csv("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/" + chunk).loc[:, get_columns(user_query_list)]
                    if "<" in cond:
                        cond = user_query_list[condidx].split("<")
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
                    elif ">" in cond:
                        cond = user_query_list[condidx].split(">")
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
                    elif "=" in cond:
                        cond = user_query_list[condidx].split("=")
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
                if not os.path.exists("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/bunch_agg_chunks/has_chunks/" + chunk[:-4] + "_has.pkl")
        else:
             for chunk in os.listdir("./" + user_query_list[0] + "_chunks/bunched_chunks"):
                if os.path.isfile("./" + user_query_list[0] + "_chunks/bunched_chunks/" + chunk):
                    table = pd.read_csv("./" + user_query_list[0] + "_chunks/bunched_chunks/" + chunk).loc[:, get_columns(user_query_list)]
                    if "<" in cond:
                        cond = user_query_list[condidx].split("<")
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
                    elif ">" in cond:
                        cond = user_query_list[condidx].split(">")
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
                    elif "=" in cond:
                        cond = user_query_list[condidx].split("=")
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
               
                if not os.path.exists("./" + user_query_list[0] + "_chunks/bunched_chunks/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/bunched_chunks/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/bunched_chunks/has_chunks/" + chunk[:-4] + "_has.pkl")
    elif not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
        if "COLUMNS" in uqlupper:
             for chunk in os.listdir("./" + user_query_list[0] + "_chunks/col_agg"):
                if os.path.isfile("./" + user_query_list[0] + "_chunks/col_agg/" + chunk):
                    table = pd.read_csv("./" + user_query_list[0] + "_chunks/col_agg/" + chunk).loc[:, get_columns(user_query_list)]
                    if "<" in cond:
                        cond = user_query_list[condidx].split("<")
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
                    elif ">" in cond:
                        cond = user_query_list[condidx].split(">")
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
                    elif "=" in cond:
                        cond = user_query_list[condidx].split("=")
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
                
                if not os.path.exists("./" + user_query_list[0] + "_chunks/col_agg/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/col_agg/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/col_agg/has_chunks/" + chunk[:-4] + "_has.pkl")
        else:
           ### Copypaste this above
            for chunk in os.listdir("./" + user_query_list[0] + "_chunks/agg"):
                if os.path.isfile("./" + user_query_list[0] + "_chunks/agg/" + chunk):
                    table = pd.read_csv("./" + user_query_list[0] + "_chunks/agg/" + chunk).loc[:, get_columns(user_query_list)]
                    if "<" in cond:
                        cond = user_query_list[condidx].split("<")
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
                    elif ">" in cond:
                        cond = user_query_list[condidx].split(">")
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
                    elif "=" in cond:
                        cond = user_query_list[condidx].split("=")
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
                if not os.path.exists("./" + user_query_list[0] + "_chunks/agg/has_chunks"):
                    os.mkdir("./" + user_query_list[0] + "_chunks/agg/has_chunks")
                table.to_pickle("./" + user_query_list[0] + "_chunks/agg/has_chunks/" + chunk[:-4] + "_has.pkl")
    elif "COLUMNS" in uqlupper:
        ### Copypaste this above
        for chunk in os.listdir("./" + user_query_list[0] + "_chunks"):
            if os.path.isfile("./" + user_query_list[0] + "_chunks/" + chunk):
                table = pd.read_csv("./" + user_query_list[0] + "_chunks/" + chunk).loc[:, get_columns(user_query_list)]
                if "<" in cond:
                    cond = user_query_list[condidx].split("<")
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
                elif ">" in cond:
                    cond = user_query_list[condidx].split(">")
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
                elif "=" in cond:
                    cond = user_query_list[condidx].split("=")
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
            if not os.path.exists("./" + user_query_list[0] + "_chunks/has_chunks"):
                os.mkdir("./" + user_query_list[0] + "_chunks/has_chunks")
            table.to_pickle("./" + user_query_list[0] + "_chunks/has_chunks/" + chunk[:-4] + "_has.pkl")
    return table
    
def sort(user_query_list):
    sorted_df = pd.DataFrame()
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
        directory = find_directory(user_query_list)
        directory = sort_within_chunks(user_query_list, sortcol, directory)
        sorted_df = sort_between_chunks(user_query_list, sortcol, directory)
    if direction == "ASC":
        return sorted_df
    elif direction == "DESC":
        return sorted_df.iloc[::-1]

def simple_sort(user_query_list, sortcol, table):
    # Convert table to list
    table_list = table.values.tolist()
    # Store schema
    tblcolumns = table.columns
    # Get sorted list-of-lists back from merge_sort and merge_asc
    sortcol_index = tblcolumns.get_loc(sortcol)
    sorted_table = merge_sort(table_list, sortcol_index)
    # Convert back to df and apply schema + datatypes
    sorted_table = pd.DataFrame(sorted_table, columns=tblcolumns)
    return sorted_table

def sort_bunch(user_query_list, sortcol):
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    agg_present = not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list))))
    # Figure out bunching column
    bunchcol = user_query_list[list(map(str.upper, user_query_list)).index("BUNCH") + 1] + "_bunched"
    # Get file path of chunks
    if agg_present:
        file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunch_agg_chunks")
    else:
        file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunched_chunks")
    # For each chunk, separate bunches into different tables
    sorted_chunk_directory = "./"+ user_query_list[0] + "_chunks/sorted_chunks"
    for chunk in os.listdir(file_path):
        if os.path.isfile(file_path + "/" + chunk):
            table = pd.read_pickle(file_path + "/" + chunk)
            print("bunchcol: ", bunchcol[:-8])
            print("sortcol: ", sortcol)
            if bunchcol[:-8] == sortcol:
                print("bunching and sorting on same column")
                sorted_chunk = table.sort_index(level=bunchcol)
                print("sorted chunk: ", sorted_chunk)
                if not os.path.exists(sorted_chunk_directory):
                    os.mkdir(sorted_chunk_directory)
                sorted_chunk.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted_on_bunch.csv")
            else:
                indexes = table.index.get_level_values(0).unique().tolist()
                tbl_num = 1
                print(indexes)
                sortcol_index = table.columns.get_loc(sortcol)
                for i in indexes:
                    newtbl = table.loc[str(i), :]
                    newtbl = merge_sort(newtbl.values.tolist(), sortcol_index)
                    print("newtbl inside sort_bunch function: ", newtbl)
                    newtbl = pd.DataFrame(newtbl)
                    newtbl.columns = table.columns
                    newtbl.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted_level_" + str(tbl_num) + ".csv")
                    tbl_num += 1
    dflist = []
    for filename in sorted(os.listdir(sorted_chunk_directory)):
        if bunchcol[:-8] != sortcol:
            if "level" in filename:
                sorted_chunk = pd.read_csv(sorted_chunk_directory + "/" + filename, index_col=0)
                print("sorted chunk being read in order: ", sorted_chunk)
                dflist.append(sorted_chunk)
        else:
            if "sorted_on_bunch" in filename:
                sorted_chunk = pd.read_csv(sorted_chunk_directory + "/" + filename, index_col=0)
                print("sorted chunk being read in order: ", sorted_chunk)
                dflist.append(sorted_chunk)
    final_sorted_table = pd.concat(dflist, ignore_index=True)
    groups = final_sorted_table[bunchcol[:-8]].unique()
    tbldict = {group: final_sorted_table[final_sorted_table[bunchcol[:-8]] == group] for group in groups}
    final_sorted_table = pd.concat([v for v in tbldict.values()], keys = [k for k in tbldict.keys()], names=[bunchcol, 'ROWID'])
    return final_sorted_table

def sort_merge(user_query_list, sortcol):
    tbl1 = user_query_list[0]
    tbl2 = user_query_list[list(map(str.upper, user_query_list)).index("MERGE") + 1]
    # Run simple_sort on both tables (for all chunks)
    directory1 = "./" + tbl1 + "_chunks"
    directory2 = "./" + tbl2 + "_chunks"
    if not os.path.exists(directory1 + "/merged_tables"):
        os.mkdir(directory1 + "/merged_tables")
    merged_directory1 = directory1 + "/merged_tables"
    if not os.path.exists(directory2 + "/merged_tables"):
        os.mkdir(directory2 + "/merged_tables")
    merged_directory2 = directory2 + "/merged_tables"
    for filename in os.listdir(directory1):
        if os.path.isfile(filename):
            newtbl = pd.read_csv(directory1 + "/" + filename, index_col=0)
            newtbl = simple_sort(user_query_list, sortcol, newtbl)
            newtbl.to_csv(merged_directory1 + "/" + filename)
    for filename in os.listdir(directory2):
        if os.path.isfile(filename):
            newtbl2 = pd.read_csv(directory2 + "/" + filename, index_col=0)
            newtbl2 = simple_sort(user_query_list, sortcol, newtbl2)
            newtbl2.to_csv(merged_directory2 + "/" + filename)
    #final_merged_table = pd.DataFrame()
    df_list = []
    for left_chunk, right_chunk in zip(os.listdir(merged_directory1), os.listdir(merged_directory2)):
        print("zipping sorted chunks")
        left = pd.read_csv(directory1 + "/" + left_chunk, index_col=0)
        right = pd.read_csv(directory2 + "/" + right_chunk, index_col=0)
        matching_tuples = [pd.concat([t1, t2], axis=1, columns=[left.columns, right.columns]) for t1, t2 in zip(left, right) if t1[sortcol] == t2[sortcol]]
        print("matching tuples: ", matching_tuples)
        merged_table = pd.DataFrame(matching_tuples)
       
        df_list.append(merged_table)
    final_merged_table = pd.concat(df_list)
    return final_merged_table

def sort_within_chunks(user_query_list, sortcol, directory):
    tablename = user_query_list[0]
    uqlupper = list(map(str.upper, user_query_list))
    table = pd.DataFrame()
    for filename in os.listdir(directory):
        if "MERGE" in uqlupper:
            table = pd.read_csv(os.path.join(directory, filename), index_col=0)
        else:
            table = pd.read_pickle(os.path.join(directory, filename))
        table = simple_sort(user_query_list, sortcol, table)
        table.to_csv("./" + tablename + "_chunks/sorted_chunks/" + filename[:-4] + "_sorted.csv")
    return "./" + tablename + "_chunks/sorted_chunks"

def sort_between_chunks(user_query_list, sortcol, directory):
    tablename = user_query_list[0]
    if not os.path.exists("./"+ tablename + "_chunks/chunk_subsets"):
        os.mkdir("./"+ tablename + "_chunks/chunk_subsets")
    for filename in os.listdir(directory):
        if os.path.isfile(directory + "/" + filename):
            file_subset = pd.read_csv(directory + "/" + filename, nrows = 450, index_col=0)
            file_subset_name = f"{filename.split('.')[0]}_subset.pkl"
            file_subset_path = os.path.join("./"+ tablename + "_chunks/chunk_subsets", file_subset_name)
            file_subset.to_pickle(file_subset_path)
    subset_files = os.listdir("./"+ tablename + "_chunks/chunk_subsets")
    while len(subset_files) > 1:
        chunk1 = pd.read_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", subset_files.pop(0))) #get the first file 
        chunk2 = pd.read_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", subset_files.pop(0))) #second file 
        pd.DataFrame(merge_asc(chunk1.values.tolist(), chunk2.values.tolist(), sortcol)).to_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", f"merged_subset_{len(subset_files) + 1 }.pkl"))
        subset_files.append(f"merged_subset_{len(subset_files) + 1 }.pkl")
    final_merged_table = pd.read_pickle(os.path.join("./"+ tablename + "_chunks/chunk_subsets", subset_files[0]))
    return final_merged_table

def merge_sort(table_list, sortcol_index): 
    print("table_list in merge sort", table_list)
    if len(table_list) == 1:
        return table_list
    mid = len(table_list)//2
    left = merge_sort(table_list[:mid], sortcol_index)
    right = merge_sort(table_list[mid:], sortcol_index)
    sorted_table = merge_asc(left, right, sortcol_index)
    print("final sorted table: ", sorted_table)
    return sorted_table

def merge_asc(left, right, sortcol_index): 
    print("left list", left)
    print("right list", right)
    sorted_table = [] 
    i = 0 
    j = 0 
    while i < len(left) and j < len(right):
        print("left table sorting item: ", left[i][sortcol_index])
        print("right table sorting item: ", right[j][sortcol_index])
        if left[i][sortcol_index] < right[j][sortcol_index]:
            sorted_table.append(left[i])
            print("sorted table after appending left item - left is less than right \n", sorted_table)
            i = i + 1 
        else: 
            sorted_table.append(right[j])
            print("sorted table after appending right item - right is less than left \n", sorted_table)
            j = j + 1 
    while i < len(left): 
        sorted_table.append(left[i])
        print("sorted table after appending left item - no more items in right list \n", sorted_table)
        i = i + 1 
    while j < len(right): 
        sorted_table.append(right[j])
        print("sorted table after appending right item - no more items in left list \n", sorted_table)
        j = j + 1
    return sorted_table

if __name__ == "__main__": 
    print(os.getcwd())
    os.chdir("./DSCI-551-Final-Proj-Rel/test_db")
    user_query_list = "test merge test_new incommon col1".split()
    fetch(user_query_list)