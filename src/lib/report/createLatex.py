from logs import logDecorator as lD
import jsonref
import pylatex
from pylatex import Document, Section, Subsection, Tabular,  Tabularx, MultiColumn, MultiRow, NoEscape
import scipy
import pickle
import numpy as np
import pandas as pd
import os

config = jsonref.load(open('../config/config.json'))
logBase = config['logging']['logBase'] + '.lib.report.createLatex'

# generateTEX and addSections might not be necessary since pylatex already does it. 

@lD.log(logBase + '.addSections')
def addSections(logger, doc, num):
    """
    Adds the required sections from the associated jsonConfig file
    as listed in the 'num' argument. 
    
    Args:
        logger (TYPE): Description
        doc (LaTeX document): a latex document to write to
        num (LIST): A list of numbers specifying which sections 
        to write
    
    Returns:
        TYPE: Description
    """
    try:
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        if type(num) is not list: num = [num]
        sections = jsonConfig["sections"]
        for n in num:
            section = sections["section" + str(n)]
            print("Now writing section" + str(n))
           
            with doc.create(Section(section["title"])):
                # doc.append(NoEscape(section["text"]))
                doc.append(NoEscape(r"\lipsum[1-5]")) # For testing
                # if there are subsections, recurse? 
        return 

    except Exception as e: 
        logger.error(f'Unable to add a section. \n {e}')


@lD.log(logBase + '.saveTable')
def saveTable(logger, table, fname = None):
    """
    Args:
        logger (TYPE): Description
        table (TYPE): Description
        fname (None, optional): save file name
    
    Returns:
        TYPE: Description
    """
    try:    
        jsonConfig = jsonref.load(open('../config/paper0/createLatex.json'))
        # if fname == None: fname = jsonConfig["input"]["texfilespath"] + "sampleoutput"

        dfs = [] # for merging multiple tables
        dlen = [] # for tracking number of columns for each merged table and adding a cmidrule
        for k in table.keys():
            tbl = table[k]
            print("Now loading {}.".format(tbl))
            loadfile = "{}{}.{}".format(jsonConfig["input"]["filepath"],
                                        tbl,
                                        jsonConfig["input"]["fileformat"])
            data = pickle.load(open(loadfile, "rb"))
            dlen.append(len(data.columns))
            dfs.append(data)
        mergedtable = pd.concat(dfs, axis=1, keys=table.keys(), sort=False)
        mergedtable.fillna(0, inplace=True)
        with open(fname + '.tex','w') as tf:
            tf.write(r"\centering")
            tf.write(mergedtable.to_latex(multirow=True, float_format="%0.0f")) #, column_format=r"0.9\textwidth"))
        return 

    except Exception as e: 
        logger.error(f'Unable to save a table. \n {e}')

@lD.log(logBase + '.addTable')
def addTable(logger, doc, fpath, tblno, wideTable=False):
    try:
        tblName = "Table {}".format(tblno) 
        tblPath = os.path.join(r"Paper1/table" + str(tblno))
        print(tblPath)
        with doc.create(Subsection(tblName)) as tbl:
            if wideTable: 
                tbl.append(NoEscape(r"\setlength{\tabcolsep}{2pt}"))
                tbl.append(NoEscape(r"\resizebox{\textwidth}{!}{"))
            tbl.append(NoEscape(r"\input{" + tblPath + r".tex}")) 
            if wideTable: 
                tbl.append(NoEscape(r"}"))
        return 
    except Exception as e: 
        logger.error(f'Unable to add the table. \n {e}')


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