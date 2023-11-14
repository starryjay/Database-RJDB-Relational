import os

def drop(user_input_query):
    tablename = user_input_query[0]
    filename = tablename + ".pkl"
    if os.path.exists("./table"):
        if filename in os.listdir("./table"):
            os.remove("./table/" + filename)
            print("Dropped table", tablename)