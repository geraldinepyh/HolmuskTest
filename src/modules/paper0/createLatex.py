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
        # doc = arxivReport() # generates entire
        
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        table1 = jsonConfig["tables"]["table1"]["files"]
        # Creates Tables
        lt.saveTable(table = table1, fname = "table2sample")
        # Creates PDF Document
        stringLatex()

        return
    except Exception as e: 
        logger.error(f'Unable to run main \n {e}')

@lD.log(logBase + '.stringLatex')
def stringLatex(logger):
    '''
    String pieces of latex together to form a report. 
    Assumes Latex have already been written to the same output location.
    '''
    try:
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        fpath = jsonConfig['output']['savePath']
        texfiles = jsonConfig['input']['texfilespath']

        # # Configure Document 
        doc = Document()
        packages = jsonConfig["packages"]
        for p in packages: doc.packages.append(Package(p))
        preambles = jsonConfig["preamble"]
        # Add Title/Authors
        for pa in preambles:
            doc.preamble.append(Command(pa, preambles[pa]))
        doc.append(NoEscape(r"\maketitle"))
        # Abstract          
        doc.append(NoEscape(r"\begin{abstract}"))
        doc.append(jsonConfig["abstract"])
        doc.append(NoEscape(r"\end{abstract}"))

        # # # String pieces
        doc.append(NoEscape(r"\input{sampleoutput}")) 
        # different file path because of where pdflatex is called to compile

        # # Build the document
        doc.generate_tex(fpath+"stringedPieces")

        return doc
    except Exception as e: 
        logger.error(f'Unable to run stringLatex \n {e}')

@lD.log(logBase + '.arxivReport')
def arxivReport(logger):
    '''
    creates an arxiv-style latex report programmatically 
    based on input specified from the createLatex jsonConfig file
    Generates tables from scratch instead of reading from a file.
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
        doc.append(NoEscape(r"\begin{abstract}"))
        doc.append(jsonConfig["abstract"])
        doc.append(NoEscape(r"\end{abstract}"))
        
        # Add sections 
        lt.addSections(doc, [1, 2])

        # Tables
        table1 = jsonConfig["input"]["table1"]
        lt.addTable(doc, table1)



        # # Build the document
        doc.generate_tex(fpath+"test_arxiv")
        # # doc.generate_pdf(fpath+"test_output")

        return doc
    except Exception as e: 
        logger.error(f'Unable to create arxiv report \n {e}')

# To Remove? 
@lD.log(logBase + '.getDataFromSQL')
def getDataFromSQL(logger):
    '''download data

    This function makes a connection, downloads the data from the database.
    Then saves it to the raw data folder. 
    
    Parameters
    ----------
    logger : {logging.Logger}
        The logger used for logging error information
    '''

    print('In getDataFromSQL module.')

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
        logger.error(f'Unable to run getDataFromSQL \n {e}')

