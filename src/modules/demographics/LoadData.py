from logs import logDecorator as lD 
import jsonref, pprint
import os

from lib.databaseIO import pgIO 
from psycopg2 import sql
import numpy as np
import pandas as pd

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.demographics.LoadData'
makeConnection = jsonref.load(open('../config/db.json'))


@lD.log(logBase + '.LoadData')
def LoadData(logger, argParam):
    '''download data

    This function makes a connection, downloads the data from the database. 
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    print('We are in LoadData module.')

    try:
        print('hi')
        jsonConfig = jsonref.load(open('../config/modules/loadData.json'))
        print('hEre I am.')
        schema = jsonConfig['saveData']['schema']
        table = jsonConfig['saveData']['table']
        saveFolder = jsonConfig['saveData']['saveFolder']
        
        query = sql.SQL('''
                        SELECT *
                        FROM {schema}.{table}
                        ''').format(schema  = sql.Identifier(schema), 
                                    table   = sql.Identifier(table))

        data = pgIO.getAllData(query)

        # Check that the data is properly loaded
        print("-" * 10)
        

        data = np.array(data)
        # Save the data to the /data/raw folder
        np.save( os.path.join(saveFolder, 'raw_data.npy'), data)

        return data

    except Exception as e: 
        logger.error(f'Unable to run LoadData \n {e}')


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

    LoadData(resultsDict['module2'])

    print('Getting out of LoadData')
    print('-'*30)

    return

