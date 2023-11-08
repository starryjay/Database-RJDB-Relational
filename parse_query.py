import make, edit, fetch 
import os
abspath = "/Users/roma/Documents/USC/Fall 2023 /Data Management /DSCI-551-Final-Proj-Rel"

def parse_query(user_input_string): 
    os.chdir(abspath)
    if not os.path.exists('./table'):
        os.mkdir('./table')
    os.chdir('./table')
    user_input_list = user_input_string.replace(',', '').split()
    word = user_input_list[0] 
    if word.upper() == "MAKE": 
        return make.make(user_input_list[1:])
    elif word.upper() == "EDIT": 
        return edit.edit(user_input_list[1:])
    elif word.upper() == "FETCH": 
        return fetch.fetch(user_input_list[1:])
    else:
        raise ValueError("Invalid query - use MAKE, EDIT, or FETCH!")
