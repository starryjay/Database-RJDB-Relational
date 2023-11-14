from make_tbl import make
from edit_tbl import edit
from fetch_tbl import fetch
from drop_tbl import drop
import os

def parse_query(user_input_string, current_db):
    print("Current db:", os.path.split(os.getcwd())[1])
    if os.path.split(os.getcwd())[1] != current_db:
        os.chdir("../" + current_db)
    user_input_list = user_input_string.replace(',', '').split()
    print("Input list: ", user_input_list)
    first_keyword = user_input_list[0] 
    print("first keyword:", first_keyword.upper())
    if first_keyword.upper() == "MAKE": 
        return make(user_input_list[1:])
    elif first_keyword.upper() == "EDIT": 
        print("Entering edit function")
        return edit(user_input_list[1:], current_db)
    elif first_keyword.upper() == "FETCH": 
        return fetch(user_input_list[1:])
    elif first_keyword.upper() == "DROP":
        return drop(user_input_list[1:])
    else:
        print("Invalid query. If modifying databases, use MAKEDB, USEDB, or DROPDB. If modifying tables, use MAKE, EDIT, FETCH, or DROP.")