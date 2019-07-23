from logs import logDecorator as lD 
import jsonref, pprint
import os

from lib.databaseIO import pgIO 
from psycopg2 import sql
import numpy as np
import pandas as pd
import pickle

from lib.report import createLatex as lt
import pylatex
from pylatex import Document, Section, Subsection, Tabular, MultiColumn, MultiRow, Package, NoEscape
from tabulate import tabulate 

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.paper0.createLatex'


@lD.log(logBase + '.main')
def main(logger, resultsDict):
    '''
    Testing to generate the latex pdf 
    '''
    try:
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        fpath = jsonConfig['output']['savePath']
        doc = Document()
        doc.packages.append(Package("booktabs")) #to read the \toprule control sequences
        section = Section("Tables")
        subsection = Subsection(jsonConfig['Sub-Section1Title'])
        subsection2 = Subsection("Table1")

        # add a testing table
        data = getData() 
        columns = ["id","race","sex","siteid"]
        table0 = lt.generateTable(data, columns) 
        subsection.append(table0)

        subsection.append("\n The above is a testing table for a simple query.")

        # Table 1 
        out10 = pickle.load(open("../data/final/jwpickles/out10.pickle", "rb"))
        subsection2.append(NoEscape(out10.to_latex(multirow=True)))
        subsection2.append("\n See awesome Table~\ref{tab:out10}.")

        section.append(subsection)
        section.append(subsection2)

        # Build the document
        doc.append(section)
        doc.generate_tex(fpath+"test_output") # from pylatex
        doc.generate_pdf(fpath+"test_output")

        print('-'*10, 'Pdf generated.','-'*10)
        return
    except Exception as e: 
        logger.error(f'Unable to run main \n {e}')

@lD.log(logBase + '.getData')
def getData(logger):
    '''download data

    This function makes a connection, downloads the data from the database.
    Then saves it to the raw data folder. 
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    print('In getData module.')

    try:
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        schema = jsonConfig['query']['schema']
        table = jsonConfig['query']['table']
        
        print("Starting Query now.")
        query = '''
                SELECT id, race, sex, siteid --*
                FROM {schema}.{table}
                WHERE race IS NOT NULL
                LIMIT 10
                '''.format(schema  = schema, 
                            table   = table)
        data = pgIO.getAllData(query)
        data = np.array(data)
        # print(data)
        return data

    except Exception as e: 
        logger.error(f'Unable to run getData \n {e}')

