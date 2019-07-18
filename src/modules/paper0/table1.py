from logs import logDecorator as lD 
import jsonref, pprint
import os

from lib.databaseIO import pgIO 
from psycopg2 import sql
import numpy as np
import pandas as pd

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.paper0.table1'


@lD.log(logBase + '.getQueryData')
def getQueryData(logger, query, limit = False):
    ''' get filtered data

    this function queries the database (specified in corresponding json file)
    and returns filtered data that it will save in the raw data folder
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''
    print('In getQueryData module.')

    try:
        jsonConfig = jsonref.load(open('../config/paper0/table1.json'))
        schema = jsonConfig['Connections']['schema']
        table = jsonConfig['Connections']['table']

        print("Starting Query now.")
        # query = sql.SQL(makeQueryString(limit=True))
        if limit: 
            query += " LIMIT 50"
            print(query)
        data = pgIO.getAllData(sql.SQL(query))
        
        print('Got my data, it has {} records'.format(len(data)))
        # persist data to intermediate folder
        df = pd.DataFrame(data)
        return df

    except Exception as e: 
        logger.error(f'Unable to run getQueryData \n {e}')

@lD.log(logBase + '.queryExtend')
def queryExtend(logger, col_name, col_values):
    """Summary
    This function takes a column name and creates an sql like extension
    e.g. queryExtend('alphabets', ['A','B','C']) will return a string:
        "AND alphabets IN ('A','B','C')"

    Parameters
    ----------
    logger : TYPE
        Description
    col_name : string
        name of column to query
    col_values : list of values
        possible values
    
    Returns
    -------
    string
        string in SQL that can be appended to your query
    """
    try:
        query_extension = " AND {} IN (".format(col_name)
        ext=""
        for r in col_values:
            ext += "\'" + str(r) + "\', "
        query_extension += ext[:-2] + ")"
        return query_extension 

    except Exception as e: 
        logger.error(f'Unable to run queryExtend \n {e}')

@lD.log(logBase + '.recodeData')
def recodeData(logger, data, column):
    '''generate a string of SQL query to filter specific fields according to the given columns    
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    data : pd.dataframe
        This contains the data that you want to recode.
    columns : list
        name of the columns you want to recode.
        There should be a matching file under the data/raw_data/ref/ folder in csv format.
        This file will contain 2 columns: (1) Expected Values, (2) Values to Recode
     
    Returns
    -------
    TYPE
        Description    
    '''
    print('In recodeData module.')

    try:

        ref = pd.read_csv('../data/raw_data/ref/{}.csv'.format(column), index_col=False)
        data_merged = data.merge(right=ref, how = 'inner', on=column)
        data_merged = data_merged.drop(column,axis=1)
        return data_merged

    except Exception as e: 
        logger.error(f'Unable to run recodeData \n {e}')


@lD.log(logBase + '.main')
def main(logger, resultsDict):
    '''main function to create Table 1
    '''
    print('-'*30)
    try:
        jsonConfig = jsonref.load(open('../config/paper0/table1.json'))
        schema = jsonConfig['Connections']['schema']
        table = jsonConfig['Connections']['table']
        reference_file = pd.read_csv("../data/raw_data/ref/race.csv")

        # Filter data from background using race.csv to get patients 
        print('1. Filtering patients by race and sex..')
        q1 = '''
                SELECT t1.id, t1.race, t1.sex, t1.siteid, tp.age  
                FROM {schema}.{table} t1
                LEFT JOIN raw_data.typepatient tp 
                ON t1.id = tp.backgroundid
                WHERE race IS NOT NULL
                AND sex IS NOT NULL
                AND CAST(tp.age as INTEGER) < 91
                AND tp.visit_type in ('Inpatient','Outpatient')
            '''.format(schema  = schema, 
                        table   = table) + queryExtend('race', reference_file['race'])
        data = getQueryData(q1, limit=True)
        data.columns = ['id','race','sex','siteid','age']

        print('Recoding race and sex..')
        for col in ['race','sex']: data = recodeData(data, col) 
        print(data.head())

        print('Filtering by clinical setting')
        #######

        # Use patient ids to get diagnoses
        print('3. Getting all diagnoses of filtered patients..')

        # Filter diagnoses using dsmno.csv
        print('Filtering patients by dsmno..') 

        dsmno_file = pd.read_csv("../data/raw_data/ref/dsmno.csv")
        q2 = """
            SELECT backgroundid, dsmno --, onset_date
            FROM raw_data.pdiagnose 
            where dsmno is not null 
            """ + queryExtend('backgroundid', data['id']) + queryExtend('dsmno', dsmno_file['dsmno'])
        data_dsmno = getQueryData(q2,limit=True)
        data_dsmno.columns = ['id', 'dsmno']
        print(data_dsmno.head())

        # Recode diagnoses from dsmno to category & OHE 
        # Result: each id, 15 columns for each mental_DSM
        # + each id, 11 columns for each sud_DSM
        

        print('-'*30)

    except Exception as e: 
        logger.error(f'Unable to run main \n {e}')

    return

