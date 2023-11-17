import os 
import pandas as pd

def sort(user_query_list, table=None):
    if table is not None:
        print("this is table\n", table)
        sortcol = user_query_list[list(map(str.upper, user_query_list)).index("SORT") + 1]
        print("this is col to sort on \n", sortcol)
        bunchcol = user_query_list[list(map(str.upper, user_query_list)).index("BUNCH") + 1]
        if sortcol == bunchcol:
            print("sort and bunch col are same ")
            # sort the index keys
            sortedpath = sort_chunk(user_query_list, sort_is_bunch=True)
        else:
            # sort within groups
            sortedpath = sort_chunk(user_query_list, sort_is_bunch=False)
    else:
        sortedpath = sort_chunk(user_query_list)
        #print("sorted path: ", sortedpath)
    return sort_chunk_by_limit(sortedpath, user_query_list)

def sort_chunk(user_query_list, sort_is_bunch=False):
    if not os.path.exists("./"+ user_query_list[0] + "_chunks/sorted_chunks"):
        os.mkdir("./"+ user_query_list[0] + "_chunks/sorted_chunks")
    sortedpath = "./"+ user_query_list[0] + "_chunks/sorted_chunks"
    agglist = ["TOTALNUM", "SUM", "MEAN", "MIN", "MAX"]
    if "BUNCH" in user_query_list:
        if not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
            file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunch_agg_chunks")
        else:
            file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "bunched_chunks")
    elif not set(agglist).isdisjoint(set(list(map(str.upper, user_query_list)))):
        file_path = os.path.join("./"+ user_query_list[0] + "_chunks", "col_agg")
    else:
        file_path = "./"+ user_query_list[0] + "_chunks"
    for filename in os.listdir(file_path):
        if os.path.isfile(file_path + "/" + filename):
            if sort_is_bunch:
                bunchcol = user_query_list[list(map(str.upper, user_query_list)).index("BUNCH") + 1] + "_bunched"
                table = pd.read_pickle(file_path + "/" + filename)
                sorted_chunk = table.sort_index(level=bunchcol)
            else:
                sorted_chunk = sort_direction(user_query_list, os.path.join(file_path, filename))
            sorted_chunk.to_csv("./" + user_query_list[0] + "_chunks/sorted_chunks/" + filename[:-4] + "_sorted.csv", index=False)
    return sortedpath

def sort_chunk_by_limit(sorted_chunks_directory, user_query_list):
    sort_col = user_query_list[list(map(str.upper, user_query_list)).index("SORT") + 1]
    if not os.path.exists("./"+ user_query_list[0] + "_chunks/chunk_subsets"):
        os.mkdir("./"+ user_query_list[0] + "_chunks/chunk_subsets")
    for filename in os.listdir(sorted_chunks_directory):
        file_subset = pd.read_csv(sorted_chunks_directory + "/" + filename, nrows = 450, index_col=0)
        file_subset_name = f"{filename.split('.')[0]}_subset.pkl"
        file_subset_path = os.path.join("./"+ user_query_list[0] + "_chunks/chunk_subsets", file_subset_name)
        file_subset.to_pickle(file_subset_path)
    subset_files = os.listdir("./"+ user_query_list[0] + "_chunks/chunk_subsets")
    #print(subset_files)
    #print("before recursion:", len(subset_files))
    while len(subset_files) > 1:
        print("Subset file length", len(subset_files))
        chunk1 = pd.read_pickle(os.path.join("./"+ user_query_list[0] + "_chunks/chunk_subsets", subset_files.pop(0))) #get the first file 
        print("chunk 1 list", chunk1.values.tolist())
        
        chunk2 = pd.read_pickle(os.path.join("./"+ user_query_list[0] + "_chunks/chunk_subsets", subset_files.pop(0))) #second file 
        print("chunk 2 list", chunk2.values.tolist())
        if "ASC" in list(map(str.upper, user_query_list)):
            combined_merged_chunk = merge_asc(chunk1.values.tolist(), chunk2.values.tolist(), sort_col)
        elif "DESC" in list(map(str.upper, user_query_list)):
            combined_merged_chunk = merge_desc(chunk1.values.tolist(), chunk2.values.tolist(), sort_col)
        pd.DataFrame(combined_merged_chunk).to_pickle(os.path.join("./"+ user_query_list[0] + "_chunks/chunk_subsets", f"merged_subset_{len(subset_files) + 1 }.pkl"))
        subset_files.append(f"merged_subset_{len(subset_files) + 1 }.pkl")
    final_merged_table_path = os.path.join("./"+ user_query_list[0] + "_chunks/chunk_subsets", subset_files[0])
    final_merged_table = pd.read_pickle(final_merged_table_path)
    return final_merged_table

def sort_direction(user_query_list, filepath, sort_is_bunch=False):
    print(user_query_list)
    if sort_is_bunch:
        sortcol = user_query_list[list(map(str.upper, user_query_list)).index("BUNCH") + 1] + "_bunched"
        print("sort col", sortcol)
    else:
        sortcol = user_query_list[list(map(str.upper, user_query_list)).index("SORT") + 1]
    table = pd.read_pickle(filepath)
    if "ASC" in user_query_list:
        print("Ascending sort ")
        
        return ascSort(table, sortcol)
    elif "DESC" in user_query_list:
        return descSort(table, sortcol)
    else:
        raise ValueError('Direction keyword missing, please specify direction to sort as ASC or DESC!')

def ascSort(table, sortcol):
    print(table)
    print(sortcol)
    if sortcol not in table.columns: 
        raise ValueError(f"Column not found")
    table_list = table.to_dict('records')
    print("table list", table_list)
    #initial split 

    sorted_table = merge_sort(table_list, sortcol, "ASC")
    print("Sorted table", sorted_table)
    sorted_table_df = pd.DataFrame(sorted_table)
    return sorted_table_df

def descSort(table, sortcol):
    if sortcol not in table.columns: 
        raise ValueError(f"Column not found")
    table_list = table.to_dict('records')
    sorted_table = merge_sort(table_list, sortcol, "DESC")
    sorted_table_df = pd.DataFrame(sorted_table)
    return sorted_table_df


def merge_sort(table_list, sort_col, direction): 
    print("table_list in merge sort", table_list)
    if len(table_list) == 1:
        return table_list
    mid = len(table_list)//2
    left = merge_sort(table_list[:mid], sort_col, direction)
    right = merge_sort(table_list[mid:], sort_col, direction)
    if direction == "ASC":
        return merge_asc(left, right, sort_col)
    elif direction == "DESC":
        return merge_desc(left, right, sort_col)

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
    while j < len(right): 
        sorted_table.append(right[j])
        j = j + 1
    while i < len(left): 
        sorted_table.append(left[i])
        i = i + 1 
    return sorted_table

if __name__ == "__main__":
    #fetch test columns col1 col2 col3 SORT col1 ASC
    os.chdir("/Users/roma/Documents/USC/Fall 2023 /Data Management /DSCI-551-Final-Proj-Rel/test_db")
    sort(["test", "columns", "col1", "col2", "col3", "SORT", "col1", "ASC"])