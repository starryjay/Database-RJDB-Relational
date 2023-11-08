'''
keep a list of aggregate functions as global variable
'''


'''
"make realestatesubset columns price=float, num_baths=int, num_beds=int"

["make", "realestatesubset", "columns", "price=float", "num_baths=int", "num_beds=int"]

if first item in list is "make":
    go into make function, cutting off "make" from list

make function:
    if first item in list is "copy" go into copy function, cutting off "copy" from list
    elif "tablename", "columns" go into generic maketable function
    else throw error

copy function:
    make pandas dataframe duplicate of existing dataframe given name in list

maketable function:
    create pandas dataframe with title as first item in list,
    column names as everything after list item "columns"

'''

'''
"edit realestatesubset insert price=[150000, 300000, 1000000], num_baths=[2, 4, 5], num_beds=[2, 3, 4]"

if first item in list is "edit":
    go into edit function, cutting off "edit" from list

edit function:
    if second item in list is "insert" go into insert function
    elif "update" go into update function
    elif "delete" go into delete function
    else throw error

insert function:
    access dataframe whose name is first item in list
    go to third item in list, after keyword "insert"
    for "column name" before equals sign, insert [data] after equals sign
        (parse list of data into pandas series)

update function (one row at a time):
    access dataframe whose name is first item in list
    go to third item in list, after keyword "update"
    for row specified after equals sign in "id=xyz", insert data at columns specified

delete function (one row at a time):
    access dataframe whose name is first item in list
    check third item in list, after keyword "delete"
    if third item is "id=x", drop row x and return table
    elif third item is "FETCH", call fetch function and then use dataframe.subtract()

'''

'''
if first item in list is "fetch":
    go into fetch function, cutting off "fetch" from list

fetch function:
    if third item in list after "tablename" is "columns" go into generic fetch columns function
    elif third item is an aggregate function as defined above, go into agg function
    elif third item is merge, go into merge function
    if "bunch" in list, go to bunch function
    if "sort" in list, go to sort function

fetch columns function:
    access dataframe whose name is first item in list
    locate columns specified after "columns" keyword
    if "has" keyword is present, filter columns based on condition
    return filtered dataframe

agg function:
    if fourth item in list is "has", filter dataframe by condition and return back to agg function
    if second item in list is "totalnum" go into count function
        (with filtered dataframe if applicable)
    elif "sum" go into sum function
        inside specific aggregate functions, check for "has" clause first and filter 
        then apply aggregation
    elif "mean" go into mean function
    elif "min" go into min function
    elif "max" go into max function

merge function:
    access dataframe whose name is first item in list
    access dataframe whose name is third item in list
    join on columns specified after fourth item in list ("INCOMMON")
    return joined dataframe up to fetch function,
        having cut off everything except "fetch", "tablename", and other unparsed query items

bunch function:
    if sort specified after bunch columns, go into sort function for that column first
    iterate with variable dict through array of dataframe/dictonary for table 1 
    check if dict[colname] is present in table 2 
       final_output = {t1c1: data, t1c2: data, joincol: data, t2c1: data, t2c2: data}
           iterate through dict (k, v) in dict
             finaloutput[k] = v 
            
            iterate through k, v of table2[dict]]
             finaloutput[k] = v 
    
    [
        {t1c1: data, t1c2: data, t1joincol: data}
        {t1c1: data, t1c2: data, t1joincol: data},  
    ]
    [
        {t2c1: data, t2c2: data, t2joincol: data}
        {t2c1: data, t2c2: data, t2joincol: data},
    ]    
'''


'''
def parse_query(string_input): 
if first word "MAKE":
    return tablename, cols, datatypes
if first word "EDIT":
    return 

def make_table(tablename <- string, columns <- list, datatypes <- list):
    returns pandas dataframe

def make_copy()

'''


'''

Order of Operations:

- MAKE
    - COPY
    - COLUMNS
- EDIT
    - INSERT/UPDATE/DELETE
- FETCH
    - COLUMNS
        - HAS
    - MERGE
        - INCOMMON
    - Aggregate functions (SUM, MEAN, MIN, MAX, TOTALNUM) should trigger a flag that signals aggregation
        - HAS
        - Apply aggregate function
    - BUNCH
    - SORT

'''

'''
driver program 
call parse query and based on initial keyword call either 
make, edit or fetch

def make(query_string):
    if query_string[:4] == "COPY":
        make_copy(query_string)
    else:
        make()


'''


 



