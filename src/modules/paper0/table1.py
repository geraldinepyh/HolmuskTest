from logs import logDecorator as lD 
import jsonref, pprint
import os

from lib.databaseIO import pgIO 
from psycopg2 import sql
import numpy as np
import pandas as pd

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.paper0.table1'


@lD.log(logBase + '.getFilteredData')
def getFilteredData(logger):
    ''' get filtered data

    this function queries the database (specified in corresponding json file)
    and returns filtered data that it will save in the raw data folder
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''
    print('In getFilteredData module.')

    try:
        jsonConfig = jsonref.load(open('../config/paper0/table1.json'))
        filename = jsonConfig['Connections']['saveFile']
        raw_data_path = os.path.join(jsonConfig['Connections']['saveFolder'],filename)
        schema = jsonConfig['Connections']['schema']
        table = jsonConfig['Connections']['table']

        # 
        print("Starting Query now.")
        query = sql.SQL('''
                        SELECT id, sex, race
                        FROM {schema}.{table}
                        WHERE race IS NOT NULL
                        ''').format(schema  = sql.Identifier(schema), 
                                    table   = sql.Identifier(table))
        data = pgIO.getAllData(query)
        print('Got my query.')
        print(len(data))

        ##><

        return data 

    except Exception as e: 
        logger.error(f'Unable to run getFilteredData \n {e}')

@lD.log(logBase + '.main')
def main(logger, resultsDict):
    '''main function for module2
    
    This function finishes all the tasks for the
    main function. This is a way in which a 
    particular module is going to be executed. 
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    resultsDict: {dict}
        A dintionary containing information about the 
        command line arguments. These can be used for
        overwriting command line arguments as needed.
    '''

    getFilteredData()

    print('-'*30)

    return

