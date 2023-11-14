import pandas as pd
import os
from printoutput import return_table
from loaddata import load_data_to_file_system, clean_data
global colnames
global dtypes

global dtype_dict

def edit(user_query_list, current_db):
    global dtype_dict
    dtype_dict = {"int": 'int64', "str": 'string', "float": 'float64', "datetime64": 'datetime64[ns]'}
    if not os.path.exists('./table/' + user_query_list[0] + ".pkl"):
        print("Invalid query - tablename does not exist!")
        return
    if user_query_list[1].upper() == 'INSERT':
        if user_query_list[2].upper() != 'FILE':
            ret = insert(user_query_list)
        else:
            ret = insert_file(user_query_list, current_db)
    elif user_query_list[1].upper() == 'DELETE':
        ret = delete(user_query_list)
    elif user_query_list[1].upper() == "UPDATE": 
        ret = update(user_query_list)
    return ret

def insert(user_query_list):
    schema = pd.read_pickle("./table/" + user_query_list[0] + ".pkl")
    global colnames
    colnames = list(schema.columns)
    global dtypes
    dtypes = list(schema.dtypes)
    record = dict([tuple(data.split('=')) for data in user_query_list[2:]])
    chunk_path = "./" + user_query_list[0] + "_chunks"
    if os.path.exists(chunk_path):
        if os.listdir(chunk_path):
            chunknolist = []
            for chunk in os.listdir(chunk_path):
                chunkno = ""
                for c in chunk: 
                    if c.isdigit(): 
                        chunkno += c 
                chunkno = int(chunkno)
                chunknolist.append(chunkno)
            last = user_query_list[0] + "_chunk" + str(max(chunknolist)) + ".csv"
            table = pd.read_csv(chunk_path+"/"+last, skiprows=1)
            if len(table) < 10000:
                table.loc[len(table)] = record
                for colname, datatype in zip(colnames, dtypes):
                    table[colname] = table[colname].astype(datatype)
                print("Inserted into existing chunk number (chunk", last, ".csv): ", record)
                return return_table(user_query_list, user_query_list[0], last)
            else:
                table = pd.DataFrame(record)
                new = user_query_list[0] + "_chunk" + str(max(chunknolist) + 1) + ".csv"
                table.to_csv(chunk_path+"/"+new)
                print("Inserted into new chunk (", new, "): ", record)
                return return_table(user_query_list, user_query_list[0], new)
    else: 
        os.mkdir(chunk_path)
        table = pd.DataFrame([record])
        table.head()
        new = user_query_list[0] + "_chunk" + str(1) + ".csv"
        table.to_csv(chunk_path+"/"+new)
        print("No chunks exist. Inserted into new chunk (", new, "): ", record)
        return return_table(user_query_list, user_query_list[0], new)
        
def insert_file(user_query_list, current_db):
    schema = pd.read_pickle("./table/" + user_query_list[0] + ".pkl")
    global colnames
    colnames = list(schema.columns)
    global dtypes
    dtypes = list(schema.dtypes)
    filename = user_query_list[3]
    df = clean_data(filename)
    load_data_to_file_system(df, current_db)
    chunk_path = "./" + user_query_list[0] + "_chunks"
    if os.path.exists(chunk_path): 
        first_chunk = os.listdir(chunk_path)[0] 
        table = pd.read_csv(first_chunk)
    for colname, datatype in zip(colnames, dtypes):
        table[colname] = table[colname].astype(datatype)
    table.to_pickle(user_query_list[0] + ".pkl")
    print("Inserted file", df.name)
    return return_table(user_query_list, user_query_list[0])

def update(user_query_list):
    schema = pd.read_pickle("./table/" + user_query_list[0] + ".pkl")
    global colnames
    colnames = list(schema.columns)[1:]
    global dtypes
    dtypes = list(schema.dtypes)[1:]
    listoftuples = [tuple(data.split('=')) for data in user_query_list[2:]]
    rownum = int(listoftuples[0][1])
    listoftuples = listoftuples[1:]
    modcols = [i[0] for i in listoftuples]
    chunkno = (rownum // 10000) + 1
    chunk_path = "./" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk" + str(chunkno) + ".csv"
    if os.path.exists(chunk_path):
        table = pd.read_csv(chunk_path)
    else:
        table = pd.DataFrame()
    for colname, datatype in zip(colnames, dtypes):
        table[colname] = table[colname].astype(datatype)
    if rownum > 10000:
        rownum = (rownum % 10000) + 1
    for i in listoftuples:
        table.loc[rownum, i[0]] = i[1]
    for i in modcols:
        for j, k in zip(colnames, dtypes):
            if i == j:
                table[i] = table[i].astype(k, copy = False)
    table.to_pickle(user_query_list[0] + ".pkl")
    table.to_csv(chunk_path)
    return return_table(user_query_list, user_query_list[0], chunkno, rownum)

def delete(user_query_list):
    schema = pd.read_pickle("./table/" + user_query_list[0] + ".pkl")
    global colnames
    colnames = list(schema.columns)
    global dtypes
    dtypes = list(schema.dtypes)
    rownum = int(user_query_list[2][3:])
    if rownum >= 10000:
        chunkno = (rownum // 10000) + 1
        chunk_path = "../" + user_query_list[0] + "_chunks/" + user_query_list[0] + "_chunk" + str(chunkno) + ".csv"
        if os.path.exists(chunk_path):
            table = pd.read_csv(chunk_path)
        global dtype_dict
        for colname, datatype in zip(colnames, dtypes):
            table[colname] = table[colname].astype(datatype)
        rownum = (rownum % 10000) + 1
    table.drop(rownum, axis='index', inplace=True)
    if int(user_query_list[2][3:]) < 10000:
        table.to_pickle(user_query_list[0] + ".pkl")
    table.to_csv(chunk_path)
    return return_table(user_query_list, user_query_list[0], chunkno, rownum)