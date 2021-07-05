import sqlite3
import os
import apsw
import io
from sqlite3 import Error

def sqlite2csv(db_file,Table_name,csv_name):   
    try:
        cmd = "sqlite3 -header -csv "
        cmd += db_file
        cmd += " \"select * from "
        cmd += Table_name
        cmd += ";\" "
        cmd += "> "
        cmd += csv_name
        os.system(cmd)  
    except Error as e:
        print(e)

def sqlite2html(db_file,Table_name,csv_name):   
    output=io.StringIO()
    conn = apsw.Connection(db_file)
    shell=apsw.Shell(stdout=output, db=conn)
    # How to execute a dot command
    shell.process_command(".mode html")
    # continue