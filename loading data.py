#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def load_csv_data(filename):
    '''This function loads andf returns the csv file'''
    return pd.read_csv(filename)

def create_database_table(conn, data, table_name):
    '''This function creates a table & if there are any data in that table, the function replaces with the new data'''

    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.commit()

def runner():
    input_csv_filename = "HP_OLTIS_Sanctioned_Budget.csv"
    db_filename = "assignment.db"

    table_name = os.path.splitext(os.path.basename(input_csv_filename))[0]

    data = load_csv_data(input_csv_filename)

    # Create or connect to the SQLite database
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Create table and insert data
    create_database_table(conn, data, table_name)

    # cursor.execute(f'SELECT * FROM {table_name}')
    # rows = cursor.fetchall()
    # for row in rows:
    #     print(row)

    conn.close()

if __name__ == "__main__":
    runner()

