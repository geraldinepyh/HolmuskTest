from logs import logDecorator as lD
import jsonref
import pylatex
from pylatex import Document, Section, Subsection, Tabular, MultiColumn, MultiRow, NoEscape
import scipy
from tabulate import tabulate  
import pickle
import numpy as np
import pandas as pd

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.lib.report.createLatex'

# generateTEX and addSection might not be necessary since pylatex already does it. 
@lD.log(logBase + '.generateTEX')
def generateTEX(logger, fpath):
    '''
    To generate the document in a TEX format
    '''
    try:
        doc = Document()
        doc.generate_tex(fpath)     
        return
    except Exception as e: 
        logger.error(f'Unable to generate the Tex. \n {e}')

@lD.log(logBase + '.addSection')
def addSection(logger, doc, title, text):
    try:
        # Create a section
        with doc.create(Section(title)):
            doc.append(text)
        return 

    except Exception as e: 
        logger.error(f'Unable to add a section. \n {e}')


@lD.log(logBase + '.addTable')
def addTable(logger, doc, table, title, caption):
    try:    
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        dfs = [] # for merging multiple tables
        for k in table.keys():
            tbl = table[k]
            print("Now loading {}.".format(tbl))
            data = pickle.load(open("{}{}.{}".format(jsonConfig["input"]["filepath"],tbl,jsonConfig["input"]["fileformat"]), "rb"))
            dfs.append(data)
        mergedtable = pd.concat(dfs, axis=1, keys=table.keys(), sort=False)
        mergedtable.fillna(0, inplace=True)
        doc.append(NoEscape(r"\setlength{\tabcolsep}{2pt}")) # Compress the table horizontally.
        doc.append(NoEscape(mergedtable.to_latex(multirow=True, float_format="%0.0f")))
        doc.append(caption)

        ## using matrix2latex()         
        # tablesection.append(NoEscape(matrix2latex(mergedtable, 
        #     caption="Testing merged PD DF", 
        #     label="pd_data_frame", 
        #     headerRow=list(table1.keys()))))
        
        return 

    except Exception as e: 
        logger.error(f'Unable to add a table. \n {e}')



@lD.log(logBase + '.generateTableFromNP')
def generateTableFromNP(logger, data, header = None):
    """
    To programmatically generate latex Tabular() object given numpy data
    Args:
        data (NumPy array): data to be converted into a latex table object
        header (list of strings): header values. if null (default), return 
        header with c0 to c(num_cols)
    Returns:
        table: LaTex Tabular() object

    Note: 
        Use matrix2latex instead. 
    """

    try:
        # Count number of headers, rows 
        num_cols = len(data[0]) #data.columns.count
        num_rows = len(data)

        table = Tabular(createHeaderString(num_cols)) 
        
        # Add header
        table.add_hline()
        if header is None:
            table.add_row(["c_" + str(i) for i in range(num_cols)])
        else: 
            table.add_row(header)

        table.add_hline()
        for r in range(num_rows): 
            table.add_row(data[r])
            table.add_hline()

        print(table)
        return table

    except Exception as e: 
        logger.error(f'Unable to generate the table. \n {e}')

def createHeaderString(num_cols):
    """
    Args:
        num_cols (integer): number of columns 
        that are expected in this table
    
    Returns:
        hdr (string): formats a string like "|c|c|" 
        for the Tabular() class of pylatex
    """
    hdr = "|"
    for i in range(num_cols): hdr += "c|"
    return hdr 