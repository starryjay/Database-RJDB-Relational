import pandas as pd 
import os 

def clean_data(filename):
    re_data = pd.read_csv(filename).dropna() 
    re_data.name = filename.strip().replace(" ", "_")
    return re_data

def load_data_to_file_system(df, currentdb=None):
    if currentdb is not None:
        if os.path.exists("./" + df.name + "_chunks"):
            print("Dataset already chunked!")
        else:
            os.mkdir("./" + df.name + "_chunks")
    i = 0
    chunk_count = 1
    for j in range(10000, len(df), 10000):
        print("Inside for loop")
        df.iloc[i:j].to_csv(os.path.join("./" + df.name + "_chunks/" + df.name + "_chunk" + str(chunk_count) + ".csv"))
        chunk_count += 1
        i += 10000
    print("Created ", str(chunk_count), " files.")
    
if __name__ == "__main__":

    re_data = clean_data("realtor-data.csv")
    load_data_to_file_system(re_data)