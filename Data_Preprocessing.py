#!/usr/bin/env python
# coding: utf-8



import pandas as pd
import logging


def clean_data(file):
    ''' Reads the input CSV file, performs data pre-processing on rows and columns, and returns a cleaned DataFrame.'''
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
    '''Splits the desired columns (in this case 'DmdCd', HOA) w.r.t '-' delimiter,
        renames the split columns and returns the re-orderd dataframe'''

    df[['DemandCode', 'Demand']] = df['DmdCd'].str.split('-', n=1, expand=True)
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
    '''Writes the processed dataframe to a new output file'''
    df.to_csv(output_file, index=False)

def runner():
    '''Orchestrates the data processing steps using the above functions and returns the final processed DataFrame.'''

    input_file = Path(__file__).parent / "himkosh_data.csv"
    output_file = Path(__file__).parent / "HP_OLTIS_Sanctioned_Budget.csv"

    budget_data = clean_data(input_file)
    budget_data = split_columns_and_rename(budget_data)

    write_to_csv(budget_data, output_file)
    df = pd.read_csv(output_file)
    
    return (df)

logging.info('Data Processing Completed')

