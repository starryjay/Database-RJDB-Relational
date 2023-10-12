import pandas as pd 
import numpy as np
import os 
import csv 






def clean_data(real_estate_filename):
    re_data = pd.read_csv(real_estate_filename).dropna() 
    return re_data
    

def load_real_estate_data_to_file_system(real_estate_df):
    os.mkdir("./chunks")
    i = 0
    chunk_count = 1
    for j in range(10000, len(real_estate_df), 10000):
        print("Inside for loop")
        real_estate_df.iloc[i:j].to_csv(os.path.join("./chunks/chunk" + str(chunk_count) + ".csv"))
        chunk_count += 1
        i += 10000
    print("Created ", str(chunk_count), " files.")

    
   
   
   
   
      
    
    
    
    
    
    
    
    
if __name__ == "__main__":
    
    
    re_data = clean_data("realtor-data.csv")
    load_real_estate_data_to_file_system(re_data)
    
    
    
    
    
   
    
    