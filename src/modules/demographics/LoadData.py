from logs import logDecorator as lD 
import jsonref, pprint

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
        query = sql.SQL('''
            SELECT patientid, religion
            FROM rwe_version1_1.background
            -- WHERE religion = 'Other'
            LIMIT 20
            ''') #.format(schema =Identifier(), table=Identifier(), var1 = Literal())

        data = pgIO.getAllData(query)

        # Check that the data is properly loaded
        print("-" * 10)
        print("The shape of the data is: ")
        print(data)

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

