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
from pylatex import Document, Section, Subsection, Tabular, Tabularx, MultiColumn, MultiRow, Package, NoEscape, Command
from matrix2latex import matrix2latex

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.modules.paper0.createLatex'


@lD.log(logBase + '.main')
def main(logger, resultsDict):
    '''
    Testing to generate the latex pdf 
    '''
    try:
        arxivReport()
        # jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        # fpath = jsonConfig['output']['savePath']
        # doc = Document()
        # doc.packages.append(Package("booktabs"))
        # doc.packages.append(Package("amsmath"))
        # doc.packages.append(Package("caption"))

        # section = Section("Tables")
        # tablesection = Subsection("Tables")

        # table1 = jsonConfig["input"]["table1"]
        # # Add from lib
        # lt.addTable(doc, table1, "Table1", "This describes the first table.")

        # # Build the Section
        # section.append(tablesection)

        # # Build the document
        # doc.append(section)
        # doc.generate_tex(fpath+"test_output") # from pylatex
        # # doc.generate_pdf(fpath+"test_output")

        print('-'*10, 'Pdf generated.','-'*10)
        return
    except Exception as e: 
        logger.error(f'Unable to run main \n {e}')

@lD.log(logBase + '.arxivReport')
def arxivReport(logger):
    '''

    '''
    try:
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        fpath = jsonConfig['output']['savePath']
        # Configure Document 
        doc = Document()
        # Load packages
        packages = jsonConfig["packages"]
        for p in packages: doc.packages.append(Package(p))
        # Preamble 
        preambles = jsonConfig["preamble"]
        for pa in preambles:
            doc.preamble.append(Command(pa, preambles[pa]))
        doc.append(NoEscape(r"\maketitle"))
        # Abstract 

        # with doc.create()


        # Generate PDF/Tex
        doc.generate_tex(fpath+"test_arxiv")
        # doc.generate_pdf('full', clean_tex=False)
        return
    except Exception as e: 
        logger.error(f'Unable to create arxiv report \n {e}')

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

