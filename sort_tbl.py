import os
import pandas as pd


# General flow of things
    # get user query list 
    # check for independent sorting or sorting with more than one kw 
        # possible kw: BUNCH (AGG), MERGE
    # for any sort, make sure to iterate through proper chunks folder
    # sort each chunk individually and store
    # then merge+sort chunks together into final table
    # for descending sort, use df.iloc[::-1] <-- reverses order of rows


def find_directory(user_query_list):
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
            else:
                if "SORT" in user_query_list:
                    filepath += sorted_tables
        if "HAS" in user_query_list:
            filepath += has_chunks      
    return filepath
    
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
    sorted_table = merge_sort(table_list, sortcol)
    # Convert back to df and apply schema + datatypes
    sorted_table = pd.DataFrame(sorted_table, columns=tblcolumns)
    return sorted_table

def sort_bunch(user_query_list, sortcol):
    from fetch_tbl import bunch
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
    chunkno = 1
    for chunk in os.listdir(file_path):
        if os.path.isfile(file_path + "/" + chunk):
            table = pd.read_pickle(file_path + "/" + chunk)
            if bunchcol[:-8] == sortcol:
                sorted_chunk = table.sort_index(level=bunchcol)
                sorted_chunk_directory = "./"+ user_query_list[0] + "_chunks/sorted_chunks"
                if not os.path.exists(sorted_chunk_directory):
                    os.mkdir(sorted_chunk_directory)
                sorted_chunk.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted.csv")
            else:
                indexes = table.index.get_level_values(0).unique().tolist()
                tbl_num = 1
                for i in indexes:
                    newtbl = table[i]
                    newtbl = merge_sort(newtbl.values.tolist(), sortcol)
                    newtbl = pd.DataFrame(newtbl)
                    newtbl.columns = table.columns
                    newtbl.to_csv(sorted_chunk_directory + "/" + chunk[:-4] + "_sorted_level_" + tbl_num + ".csv")
                    tbl_num += 1
    final_sorted_table = pd.DataFrame()
    for filename in os.listdir(sorted_chunk_directory):
        pd.concat(final_sorted_table, pd.read_csv(sorted_chunk_directory + "/" + filename))
    final_sorted_table = bunch(user_query_list, final_sorted_table)
    return final_sorted_table

def sort_merge(user_query_list, sortcol):
    # Given INCOMMON column
    # Get tablenames from uql
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
        newtbl = pd.read_csv(directory1 + "/" + filename, index_col=0)
        newtbl = simple_sort(user_query_list, sortcol, newtbl)
        newtbl.to_csv(merged_directory1 + "/" + filename)
    for filename in os.listdir(directory2):
        newtbl2 = pd.read_csv(directory2 + "/" + filename, index_col=0)
        newtbl2 = simple_sort(user_query_list, sortcol, newtbl2)
        newtbl2.to_csv(merged_directory2 + "/" + filename)
    final_merged_table = pd.DataFrame()
    for left_chunk, right_chunk in zip(os.listdir(merged_directory1), os.listdir(merged_directory2)):
        left = pd.read_csv(directory1 + "/" + left_chunk, index_col=0)
        right = pd.read_csv(directory2 + "/" + right_chunk, index_col=0)
        matching_tuples = [pd.concat([t1, t2], axis=1, columns=[left.columns, right.columns]) for t1, t2 in zip(left, right) if t1[sortcol] == t2[sortcol]]
        merged_table = pd.DataFrame(matching_tuples)
        pd.concat([final_merged_table, merged_table])
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

def merge_sort(table_list, sort_col): 
    print("table_list in merge sort", table_list)
    if len(table_list) == 1:
        return table_list
    mid = len(table_list)//2
    left = merge_sort(table_list[:mid], sort_col)
    right = merge_sort(table_list[mid:], sort_col)
    return merge_asc(left, right, sort_col)

def merge_asc(left, right, sort_col): 
    print("left list", left)
    print("right list", right)
    sorted_table = [] 
    i = 0 
    j = 0 
    while i < len(left) and j < len(right):
        if left[i][sort_col] < right[j][sort_col]:
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