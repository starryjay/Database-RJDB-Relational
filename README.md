# DSCI-551-Final-Proj-Rel

This is the codebase for the relational portion of our DSCI 551 final project.

## Flow of query parsing

`cli.py` --> `queryparse.py` --> `make_tbl`/`edit_tbl`/`fetch_tbl`/`drop_tbl.py` --> `loaddata.py` (from `edit_tbl.py`) / `printoutput.py` (from `fetch_tbl.py`)

## File contents

- `cli.py` contains the code for our command line interface, along with commands and docstrings for all commands.
- `loaddata.py` contains code for loading and chunking a `csv` file into our DBMS.
- `queryparse.py` contains a driver function for parsing queries coming from the command line, leading into the following...
    - `make_tbl.py` contains functions for creating a new table or a copy of an existing table.
    - `edit_tbl.py` contains functions for editing an existing table by inserting a row, inserting a whole `csv` file, updating a row, or deleting a row.
    - `fetch_tbl.py` contains functions for selecting, filtering, grouping, aggregating, joining, and sorting data from tables.
    - `drop_tbl.py` contains a function for dropping a table from the DBMS entirely, including any chunks that were created when loading data into it.
- `printoutput.py` contains a function for crawling through the DBMS directories to locate the correct chunks for output.