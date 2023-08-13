#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import sqlite3
import logging


def load_csv_data(filename):
    '''Loads the data from a CSV file using pandas and returns a DataFrame.'''
    return pd.read_csv(filename)

def create_database_table(conn, data, table_name):
    '''Creates an SQLite database table. If the table already exists, it will be replaced with the new data.'''
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.commit()

    input_csv_filename = os.path.join(os.path.dirname(__file__), "HP_OLTIS_Sanctioned_Budget.csv")      
    db_filename = "assignment.db"
    
    table_name = os.path.splitext(os.path.basename(input_csv_filename))[0]
    data = load_csv_data(input_csv_filename)
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    create_database_table(conn, data, table_name)
    conn.close()
    return create_database_table()

logging.info('Data Loading task completed')

