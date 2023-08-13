
import os
import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def process_data():

    def clean_data(file):
        '''This function reads the csv file, performs data pre-processing w.r.t to rows, columns and returns a clean data set '''
        df = pd.read_csv(file, header=None)
        df = df.drop([0, 1, 3])

        df.iloc[0, :] = df.iloc[0, :].str.replace("\n", " ")
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        df = df.reset_index(drop=True)

        df['DmdCd'].fillna(method='ffill', inplace=True)
        df = df[df['HOA'] != 'Total']

        df['DmdCd'] = df['DmdCd'].str.replace("\n", " ")
        df['HOA'] = df['HOA'].str.replace("\n", " ")

        return df

    def split_columns_and_rename(df):
        '''This function splits the desired columns (in this case 'DmdCd', HOA) w.r.t '-' delimiter,
            renames the splitted columns and returns the re-orderd dataframe'''

        df[['DemandCode', 'Demand']] = df['DmdCd'].str.split(
            '-', n=1, expand=True)
        df = df.drop(columns='DmdCd')

        cols1 = list(df.columns)
        df = df[cols1[-2:]+cols1[0:7]]

        split_columns = df["HOA"].str.split("-", expand=True)
        df = pd.concat([df, split_columns], axis=1)
        df = df.rename(columns={0: 'MajorHead',
                                1: 'SubMajorHead',
                                2: 'MinorHead',
                                3: 'SubMinorHead',
                                4: 'DetailHead',
                                5: 'SubDetailHead',
                                6: 'BudgetHead',
                                7: 'PlanNonPlan',
                                8: 'VotedCharged',
                                9: 'StatementofExpenditure'})

        df = df.drop([10, 11, 12], axis=1)
        df = df.drop(columns="HOA")

        cols2 = list(df.columns)
        df = df[cols2[0:2] + cols2[-10:] + cols2[2:8]]

        return df

    def write_to_csv(df, output_file):
        '''This function writes the processed data set to a new output file'''

        df.to_csv(output_file, index=False)

    def runner():
        '''This function get the inputs from the previous functions and returns the processed datafarme'''

        input_file = Path(__file__).parent / "himkosh_data.csv"
        output_file = Path(__file__).parent / "HP_OLTIS_Sanctioned_Budget.csv"

        budget_data = clean_data(input_file)
        budget_data = split_columns_and_rename(budget_data)

        write_to_csv(budget_data, output_file)
        df = pd.read_csv(output_file)
        return (df)
    print(runner())


logging.info('Data Processing')


def load_data():

    def load_csv_data(filename):
        '''This function loads andf returns the csv file'''

        return pd.read_csv(filename)

    def create_database_table(conn, data, table_name):
        '''This function creates a table & if there are any data in that table, the function replaces with the new data'''

        data.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()

    input_csv_filename = os.path.join(os.path.dirname(
        __file__), "HP_OLTIS_Sanctioned_Budget.csv")
    db_filename = "assignment.db"

    table_name = os.path.splitext(os.path.basename(input_csv_filename))[0]

    data = load_csv_data(input_csv_filename)

    # Create or connect to the SQLite database
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Create table and insert data
    create_database_table(conn, data, table_name)
    conn.close()


logging.info('Data Loading task completed')

# Create DAG
with DAG('HP_budget_processing_dag',
         start_date=datetime(2023, 8, 12),
         schedule_interval='@daily',
         catchup=False) as dag:

    # Define Task
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        dag=dag
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag
    )

process_data_task >> load_data_task
