import gzip
import shutil
import xml.etree.ElementTree as ET
import pandas as pd
import string, os, math
import numpy as np
from tqdm import tqdm
import random
from datetime import datetime
from collections import Counter
import time
from pathlib import Path
from os import listdir
import argparse
import re
import csv
import statistics
import sys
import glob
import argparse

# setup
parser = argparse.ArgumentParser()
parser.add_argument('--inputCSV', '-i', help="input CSV file of extracted data", required=True)
parser.add_argument('--inputDir', '-d', help="input directory containing parsed CUI text files", required=True)
parser.add_argument('--outputFileName', '-o', help="output file name for flatfile", required=True)
args = parser.parse_args()

# functions
def conv_to_flatfile(df,outFileName=''):
    colNames = list(df)
    cols = [df[x].tolist() for x in df]
    stopIdx = len(df.index)
    if not outFileName:
        print('Error, output file name must be provided')
        return None
    with open(outFileName, 'w') as fWrite:
        fWrite.write('FileID||NoteType||PatientID||EncounterID||TimeStamp||CUI||PreferredText||OffsetStart||OffsetStop||DomainCode||Polarity\n')
        for idx in range(0, stopIdx):
            s = ''
            for i in range(0, len(cols)-1):
                s = s + str(cols[i][idx]) + '||'
            fWrite.write(s+'\n')
    print('created output flatfile named ' + str(outFileName))

# helper section
inputDir = None
checkInput = Path(args.inputDir)
if checkInput.is_dir():
    if not os.listdir(args.inputDir):
        print('No files were found in the directory: ' + str(args.inputDir) + ', please check the directory before proceeding.')
        sys.exit()
    inputDir = args.inputDir
else:
    print('The provided input directory ' + str(args.inputDirectory) + ' is not a directory. Please check the spelling and PATH and try again.')
    sys.exit()

inputCSV = None
checkInput = Path(args.inputCSV)
if checkInput.is_file():
    inputCSV = args.inputCSV
else:
    print('Unable to locate the provided input CSV ' + str(args.inputCSV) + ', please check the spelling and PATH and try again.')
    sys.exit()

# import CUIs as list then concatenate
dfList = []
df_cuis = []
gPath = str(inputDir + '/*.txt')
fList = glob.glob(gPath)
for f in fList:
    fName = f.split('.')[0]
    fName = fName.split('/')[-1]
    # CUI,PreferredText,DomainCode,Start,End
    df = pd.read_csv(f, header=None, names=['CUI','PreferredText','DomainCode','OffsetStart','OffsetStop'])
    df['FileID'] = fName
    dfList.append(df)
df_cuis = pd.concat(df_cuis)

# import data CSV
dataCSV = pd.read_csv(inputCSV)

# merge data and reorder cols
df_final = pd.merge(df_cuis, dataCSV, how="inner", on="FileID")
df_final["Polarity"] = ''
df_final = df_final[["FileID","NoteType","PatientID","EncounterID","TimeStamp","CUI","PreferredText","OffsetStart","OffsetStop","DomainCode","Polarity"]]
df_final = df_final.copy()
conv_to_flatfile(df_final, args.outputFileName)
