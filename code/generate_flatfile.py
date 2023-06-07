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
parser.add_argument('--setCSVkey', '-k', help='value for the column of unique keys in the input CSV file', default="FileID", nargs='?', const="FileID")
parser.add_argument('--fileIDsuffix', '-f', help='file suffix for file IDs in the FileID column', default='csv', nargs='?', const='csv')
args = parser.parse_args()

csvKeyBool = False
csvKey = None
if args.setCSVkey != 'FileID':
    csvKey = str(args.setCSVkey)
    csvKeyBool = True

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
gPath = str(inputDir + '/*.txt')
fList = glob.glob(gPath)
print('importing CUI files')
for f in tqdm(fList):
    fName = f.split('.')[0]
    fName = fName.split('/')[-1]
    # CUI,PreferredText,DomainCode,Start,End
    df = pd.read_csv(f, header=None, names=['FileID', 'CUI','DomainCode','PreferredText','OffsetStart','OffsetStop'], sep='|')
    dfList.append(df)
df_cuis = pd.concat(dfList)

# import data CSV
print('importing inputCSV file')
dataCSV = pd.read_csv(inputCSV)
checkCols = list(dataCSV)

print('Now validating input CSV file.')
if csvKeyBool:
    if csvKey not in checkCols:
        if 'FileID' in checkCols:
            print('WARNING, the column name you provided for --setCSVkey does not exist,')
            print('but the column FileID exists in the input CSV file.')
            print('Using the column FileID in the inputCSV file\ninstead as the key for the union of the two datasets')
        else:
            print('Error, the column name you provided for --setCSVkey ' + str(csvKey) + '\ndoes not exist in the input CSV file,\nand there is no column named FileID.')
            print('Exiting now.')
            sys.exit()
    else:
        if 'FileID' in checkCols:
            print('Error, you provided a value for --setCSVkey but a column\nwith the name FileID\nalready exists in the input CSV file.')
            print('\nPlease check the input CSV file columns and ensure that\neither there is a column named FileID,\nor no column named FileID exists and you provide a value for --setCSVkey')
            print('Exiting now.')
            sys.exit()
        else:
            print('Using ' + str(csvKey) + ' as the unique key for the union of the two datasets')
            dataCSV = dataCSV.rename(columns={csvKey : 'FileID'})

# additional checks to ensure FileID matches in both datasets
checkFileID = dataCSV['FileID'].tolist()
cuiList = df_cuis['FileID'].tolist()
cuiList = list(set(cuiList))
cuiList = [str(x) for x in cuiList]
parsed = [x.split('.')[-1] for x in cuiList]
parsed = list(set(parsed))
if len(parsed) != 1:
    print('Error, different filename suffixes detected\nin CUI files. Exiting now.')
    sys.exit()
fSuffix = '.'+parsed[0]

checkFileID = [str(x) for x in checkFileID]
checkedList = list(filter(lambda x: x.endswith(fSuffix),checkFileID))
if len(checkedList) != len(checkFileID):
    updatedList = [str(x)+str(fSuffix) for x in checkFileID]
    dataCSV = dataCSV.rename(columns={'FileID' : 'RENAMETHENDELETE'})
    dataCSV['FileID'] = updatedList
    dataCSV = dataCSV.copy()
    dataCSV = dataCSV.drop(columns=['RENAMETHENDELETE'])

# DEBUG STEP
dataCSV = dataCSV.rename(columns={'PSEUDO_PAT_ID' : 'PatientID', 'NOTE_LAST_FILE_TIME' : 'TimeStamp', 'NOTE_TYPE' : 'NoteType', 'PSEUDO_PAT_ENC_CSN_ID' : 'EncounterID'})
# END DEBUG STEP

print('Now merging data to create final output file.')
# merge data and reorder cols
df_final = None
try:
    df_final = pd.merge(df_cuis, dataCSV, how="inner", on='FileID')
except KeyError:
    print('Error, column FileID not found in the inputCSV file.\nPlease make sure there is a column\nwith unique values named FileID in the inputCSV file,\nor you provide an argument for --setCSVkey')
    print('Exiting now.')
    sys.exit()
df_final["Polarity"] = ''
df_final = df_final[["FileID","NoteType","PatientID","EncounterID","TimeStamp","CUI","PreferredText","OffsetStart","OffsetStop","DomainCode","Polarity"]]
df_final = df_final.copy()
df_final.to_csv(args.outputFileName, index=False, sep='|')
# conv_to_flatfile(df_final, args.outputFileName)
print('job done')
