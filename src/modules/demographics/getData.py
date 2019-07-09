"""Summary

Attributes
----------
config : TYPE
    Description
logBase : TYPE
    Description
"""
from logs import logDecorator as lD 
import jsonref, pprint
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re

from lib.databaseIO import pgIO 
from psycopg2 import sql

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.demographics.getData'

@lD.log(logBase + '.getData')
def getData(logger):
    '''download data
    
    This function reads the data from the rawdata file and prints some reports about it.
    e.g. demographics data of mindlinc sample data.
    Assumption is that data is already saved in rawdata.
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    
    Returns
    -------
    TYPE
        Description
    '''

    print('In getData module.')

    try:
        jsonConfig = jsonref.load(open('../config/modules/loadData.json'))
        filename = jsonConfig['reporting']['filename']
        loadFolder = os.path.join(jsonConfig['saveData']['saveFolder'],filename)

        
        print("Retrieving data from {} now.".format(loadFolder))

        data = np.load(loadFolder, allow_pickle=True)
        print(data.shape)
        df = pd.DataFrame(data)
        colnames = getHeader()
        df.columns = colnames
        # print("Retrieved headers: ", colnames)

        ## marital status plot
        df.marital = df.marital.str.lower()
        df.marital = df.marital.fillna('Unknown').astype(str)
        df.marital.replace('\b[Ss][Ii]', 'Single', inplace=True, regex=True)
        df.marital.replace('\bunknown', 'Unknown', inplace=True, regex=True)
        df.marital.replace('Divorced/Annulled', 'Divorced', inplace=True)
        df.marital.replace('^[A-Za-z]$', 'Unknown', inplace=True, regex=True) 
        # Single Character Entries can be treated as Unknowns
        
        marital_cleaned = df.marital.value_counts()[df.marital.value_counts() >= 500]
        plt.bar(df.marital.unique()[1:6], df.marital.value_counts()[1:6])
        plt.title("Bar Charts of Top 5 Marital Status")
        plt.xlabel('Frequency')
        plt.ylabel('Marital Status')        
        plt.savefig('../results/plots/marital_status_top5.png', bbox_inches='tight') # save image

        ## income plot        
        income_col = data[:,colnames.index('income')]
        income_col = income_col.astype(str)
        regex_num = re.compile('^\d+$')  #pattern: one or more digits only
        vmatch = np.vectorize(lambda x:bool(regex_num.match(x))) # run throughout the column
        sel = vmatch(income_col) 
        income_ints = income_col[sel].astype(int) # convert to int
        income_filtered = income_ints[(income_ints >= 1000) & (income_ints <100000)]
       
        plt.hist(income_filtered, bins=20)
        plt.title("Histogram of Income levels")
        plt.xlabel('Frequency')
        plt.ylabel('Income')
        plt.ylim([0,25000])
        plt.tight_layout()
        plt.savefig('../results/plots/income_hist.png', bbox_inches='tight') # save image
        
        return data

    except Exception as e: 
        logger.error(f'Unable to run getData \n {e}')

@lD.log(logBase + '.getHeader')
def getHeader(logger):
    '''
    Parameters
    ----------
    logger : TYPE
        Description
    
    Returns
    -------
    headers : LIST 
        Description
    '''
    try:
        jsonConfig = jsonref.load(open('../config/modules/loadData.json'))
        schema = jsonConfig['saveData']['schema']
        table = jsonConfig['saveData']['table']
        headersquery = sql.SQL('''
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = {schema}
                          AND table_name   = {table}
                        ''').format(schema  = sql.Literal(schema), 
                                    table   = sql.Literal(table))     
        
        headers = pgIO.getAllData(headersquery)
        print("Inside getHeader, I got {} columns ".format(len(headers)))
        # getAllData returns list of tuples. 
        colnames = [i[0] for i in headers] # convert to a list
        print("After converting, I get ", colnames)
        return colnames

    except Exception as e: 
        logger.error(f'Unable to run getHeader \n {e}')


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
    resultsDict : {dict}
        A dintionary containing information about the 
        command line arguments. These can be used for
        overwriting command line arguments as needed.
    
    Returns
    -------
    TYPE
        Description
    '''

    getData()

    print('Getting out of getData')
    print('-'*30)

    return

