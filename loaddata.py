import pandas as pd 
import os 

def clean_data(user_query_list, filepath):
    re_data = pd.read_csv(filepath, index_col=0).dropna()
    re_data.drop_duplicates(inplace=True) 
    re_data.name = user_query_list[0]
    return re_data

def load_data_to_file_system(df, currentdb=None):
    if currentdb is not None:
        if os.path.exists("./" + df.name + "_chunks"):
            print("Dataset already chunked!")
        else:
            os.mkdir("./" + df.name + "_chunks")
    else:
        print("Not in a database, please enter a database with USEDB first")
    i = 0
    chunk_count = 1
    for j in range(10000, len(df), 10000):
        df.iloc[i:j].to_csv("./" + df.name + "_chunks/" + df.name + "_chunk" + str(chunk_count) + ".csv")
        chunk_count += 1
        i += 10000
    print("Created ", str(chunk_count), " files.")