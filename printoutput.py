import pandas as pd
import os

def find_directory(user_query_list):
    if "FETCH" in user_query_list:
        tablename = user_query_list[1]
    else:
        tablename = user_query_list[0]
    chunk_path = "./" + tablename + "_chunks"
    col_agg = "/col_agg"                          # directly under chunks
    agg = "/agg"                                  # directly under chunks
    bunch_agg_chunks = "/bunch_agg_chunks"        # directly under chunks
    bunched_chunks = "/bunched_chunks"            # directly under chunks
    merged_tables = "/merged_tables"              # directly under chunks
    sorted_tables = "/chunk_subsets"              # directly under chunks
    has_chunks = "/has_chunks"                    # could be under any of above
    
    if "FETCH" in user_query_list:
        filepath = chunk_path
        agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
        if "BUNCH" in user_query_list:
            if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
                if "MERGE" in user_query_list:
                    if "SORT" in user_query_list:
                        filepath += sorted_tables
                    else:
                        filepath += merged_tables
                else:
                    filepath += bunch_agg_chunks
            else:
                filepath += bunched_chunks
        else:
            if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
                if "MERGE" in user_query_list:
                    if "SORT" in user_query_list:
                        filepath += sorted_tables
                    else:
                        filepath += merged_tables
                else:
                    if "COLUMNS" in user_query_list:
                        filepath += col_agg
                    else:
                        filepath += agg
            if "MERGE" in user_query_list:
                if "SORT" in user_query_list:
                    filepath += sorted_tables
                else:
                    filepath += merged_tables
                    print("Merged tables", merged_tables)
            else:
                if "SORT" in user_query_list:
                    filepath += sorted_tables
        if "HAS" in user_query_list:
            filepath += has_chunks      
        return filepath

def return_table(user_query_list, agg_function = None):
    filepath = find_directory(user_query_list)
    df = pd.DataFrame()
    for chunk in os.listdir(filepath):
        if os.path.isfile(filepath + "/" + chunk) and chunk[0] != ".":
            if "/bunch_agg_chunks" in filepath:
                beg  = chunk.rfind("_") + 1
                end = chunk.rfind(".")
                if chunk[beg:end].upper() == agg_function: 
                    s = pd.read_csv(filepath + "/" + chunk)
                    df = pd.concat([df, s])
            else:
                print("filepath of current file: ", filepath + "/" + chunk)
                s = pd.read_csv(filepath + "/" + chunk)
                df = pd.concat([df, s])
    return df