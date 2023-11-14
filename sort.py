import os 
import pandas as pd

def sort(user_query_list):
    sortedpath = sort_chunk(user_query_list)
    return sort_chunk_by_limit(sortedpath)

def sort_chunk(user_query_list):
    if not os.path.exists("./chunks/sorted_chunks"):
        os.mkdir("./chunks/sorted_chunks")
    sortedpath = "./chunks/sorted_chunks"
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if "BUNCH" in user_query_list:
        if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
            file_path = os.path.join("./chunks", "bunch_agg_chunks")
        else:
            file_path = os.path.join("./chunks", "bunched_chunks")
    elif not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
        file_path = os.path.join("./chunks", "col_agg")
    else: 
        for filename in os.listdir("./chunks"):
            if not file_path:
                file_path = os.path.join("./chunks", filename)
            sorted_chunk = sort_direction(user_query_list, file_path)
            sorted_chunk.to_pickle(".chunks/sorted_chunks/" + filename + "_sorted.pkl")
    return sortedpath

def sort_chunk_by_limit(sorted_chunks_directory, user_query_list):
    sort_col = user_query_list[list(map(str.upper, user_query_list)).index("SORT") + 1]
    if not os.path.exists("./chunks/chunk_subsets"):
        os.mkdir("./chunks/chunk_subsets")
    for filename in os.listdir(sorted_chunks_directory):
         file_subset = pd.read_csv(filename, nrows = 450)
         file_subset_name = f"{filename.split('.')[0]}_subset.pkl"
         file_subset_path = os.path.join('./chunks/chunk_subsets', file_subset_name)
         file_subset.to_pickle(file_subset_path)
    subset_files = os.listdir('./chunks/chunk_subsets')
    while len(subset_files) > 1:
        chunk1 = pd.read_pickle(os.path.join('./chunks/chunk_subsets', subset_files.pop(0))) #get the first file 
        chunk2 = pd.read_pickle(os.path.join('./chunks/chunk_subsets', subset_files.pop(0))) #second file 
        if "ASC" in list(map(str.upper, user_query_list)):
            combined_merged_chunk = merge_asc(chunk1.tolist(), chunk2.tolist(), sort_col)
        elif "DESC" in list(map(str.upper, user_query_list)):
            combined_merged_chunk = merge_desc(chunk1.tolist(), chunk2.tolist(), sort_col)
        pd.DataFrame(combined_merged_chunk).to_pickle(os.path.join('./chunks/chunk_subsets', f"merged_subset_{len(subset_files) + 1 }.pkl"))
        subset_files.append(f"merged_subset_{len(subset_files) + 1 }.pkl")
    final_merged_table_path = os.path.join("./chunks/chunk_subsets", subset_files[0])
    final_merged_table = pd.read_pickle(final_merged_table_path)
    return final_merged_table

def sort_direction(user_query_list, filepath):
    sortcol = list(map(str.upper, user_query_list)).index("SORT") + 1
    if "ASC" in user_query_list:
        return ascSort(filepath, sortcol)
    elif "DESC" in user_query_list:
        return descSort(filepath, sortcol)
    else:
        raise ValueError('Direction keyword missing, please specify direction to sort as ASC or DESC!')

def ascSort(table, sortcol):
     if sortcol not in table.columns: 
         raise ValueError(f"Column not found")
     table_list = table.to_dict('records')
     sorted_table = merge_sort(table_list, sortcol, "ASC")
     sorted_table_df = pd.DataFrame(sorted_table)
     return sorted_table_df

def descSort(table, sortcol):
    if sortcol not in table.columns: 
        raise ValueError(f"Column not found")
    table_list = table.to_dict('records')
    sorted_table = merge_sort(table_list, sortcol, "DESC")
    return sorted_table

def merge_sort(table_list, sort_col, direction): 
    mid = len(table_list)//2
    right = table_list[mid:]
    left = table_list[:mid]
    right = merge_sort(right, sort_col)
    left = merge_sort(left, sort_col)
    if direction == "ASC":
        return merge_asc(left, right, sort_col)
    elif direction == "DESC":
        return merge_desc(left, right, sort_col)

def merge_asc(left, right, sort_col): 
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

def merge_desc(left, right, sort_col):
    sorted_table = [] 
    i = 0 
    j = 0 
    while i < len(left) and j < len(right):
        if left[i][sort_col] > right[j][sort_col]:
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