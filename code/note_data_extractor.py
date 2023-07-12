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


# helper functions to validate file type
def uncompressAndCopyFile(f, wDir, debugMode='quiet'):
    uncompressedF = f.split('/')[-1]
    uncompressedF = uncompressedF.split('.')[:-1]
    uncompressedF = '.'.join(uncompressedF)
    outPath = wDir + '/' + uncompressedF
    try:
        with gzip.open(f, 'rb') as f_in:
            with open(outPath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                return outPath
    except OSError:
        if debugMode != 'quiet':
            print('Not a gzipped file or unreadable file: ' + str(f))
        return False

def checkImportAsXMI(f, wDir, debugMode='quiet'):
    if not f.endswith('.gz'):
        print('XMI files must be gz-compressed. Output will be unpredictable.')
        return False
    tryUncompress = uncompressAndCopyFile(f, wDir)
    if not tryUncompress:
        # print('Unable to uncompress file ' + str(f) + ', trying to import as CSV instead...')
        return False
    fPath = tryUncompress
    try:
        print('scanning ' + str(fPath))
        importedXML = ET.parse(fPath)
    except ET.ParseError:
        if debugMode != 'quiet':
            print('Invalid XMI or XML file: ' + str(f) + ', Trying to import as CSV instead...')
        return False
    return True

def checkImportAsCSV(f, debugMode='quiet'):
    df = None
    try:
        df = pd.read_csv(f)
    except pd.errors.ParserError:
        if debugMode != 'quiet':
            print('CSV is unreadable by pandas.')
        return 0
    if len(df.index) == 0:
        if debugMode != 'quiet':
            print('File ' + str(f) + ' is readable by pandas, but it looks like an XMI file.')
        return -1
    return 1

# main validation function for file type
def checkImportFile(f=None, importMethod="xml", forceImport=False, headerDict=None, wdirName=None, debugMode='quiet'):
    if forceImport:
        if debugMode != 'quiet':
            print('Forcing import of file using provided method. I hope you know what you are doing!')
        return importMethod
    if importMethod == "xml":
        parsedFile = checkImportAsXMI(f, wdirName)
        if not parsedFile:
            parsedFile = checkImportAsCSV(f)
            if parsedFile < 1:
                if debugMode != 'quiet':
                    print('WARNING! File ' + str(f) + ' failed CSV and XMI file checks.\nThe workflow will proceed and attempt a fallback CSV import strategy.')
                return "csv"
            else:
                return "csv"
        else:
            return "xml"
    else:
        parsedFile = checkImportAsCSV(f)
        if parsedFile < 1:
            if debugMode != 'quiet':
                print('error parsing file ' + str(f) + ' as csv, trying xml instead.')
            parsedFile = checkImportAsXMI(f, wdirName)
            if not parsedFile:
                if debugMode != 'quiet':
                    print('WARNING! File ' + str(f) + ' failed CSV and XMI file checks.\nThe workflow will proceed and attempt a fallback CSV import strategy.')
                return "csv"
            else:
                return "xml"
        else:
            return "csv"

# import header file with defined fields
def importHeaderFields(f, keyList):
    d = dict()
    try:
        with open(f) as fOpen:
            for i in fOpen:
                i = i.rstrip('\r\n')
                iSplit = i.split(',')
                if len(iSplit) == 2:
                    idx = None
                    try:
                        idx = int(iSplit[1])
                    except ValueError:
                        print('Error, the numeric value in the line of the header index file:\n')
                        print(i)
                        print('\ndoes not appear to be a valid integer.\nPlease fix this problem before running the flatfile generator.\nExiting now...\n')
                        return None
                    if str(iSplit[0]) not in keyList:
                        print('WARNING, provided key not found in the required list:\n')
                        print(str(iSplit[0]))
                        print('\nThe workflow will continue, but this can have unpredicable results!')
                        time.sleep(2)
                    d[iSplit[0]] = idx
                else:
                    print('Error, two comma-separated values were not provided in the header index file for line:\n')
                    print(i)
                    print('\nPlease fix this problem before running the flatfile generator.')
                    print('The header file must be a comma-separated list of ColumnName,Index')
                    print('And must use the correct identifiers, for example:\n')
                    customDict = {'FileName' : 16, 'DocType' : 18, 'PatID' : 14, 'EncID' : 15, 'TS' : 17}
                    [print(str(k)+','+str(v)) for k,v in customDict.items()]
                    return None
    except FileNotFoundError:
        print('Error, unable to locate the provided file:\n')
        print(f)
        print('\nPlease ensure that it is in the current working directory,\nand that the filename is correct.')
        print('Exiting now...')
        return None
    return d

# additional functions
def fallbackCSV(f, cDict, debugMode='quiet', exitOnError=False):
    parsedData = []
    l = []
    with open(f) as fOpen:
        for i in fOpen:
            i = i.rstrip('\r\n')
            l.append(i)
    l_string = ','.join(l)
    for k,v in cDict.items():
        if v == -1:
            parsedData.append('SKIPPED')
        else:
            try:
                s = l_string[v]
                parsedData.append(s)
            except IndexError:
                if debugMode != 'quiet':
                    print('Unable to load and parse raw text from file ' +str(f) + '\nPlease check the file before proceeding.')
                if exitOnError:
                    sys.exit()
                parsedData = []
                break
    return parsedData

def importXMIFile(f, cDict, wDir, debugMode='quiet', exitOnError=False):
    uncompressedF = f.split('.')[:-1]
    uncompressedF = '.'.join(uncompressedF)
    uncompressedF = uncompressedF.split('/')[-1]
    fName = uncompressedF.split('.')[0]
    fPath = wDir + '/' + uncompressedF
    parsedData = []
    tree = None
    try:
        tree = ET.parse(fPath)
    except (ET.ParseError, FileNotFoundError):
        if debugMode != 'quiet':
            print('Error, XML parser was unable to read the XMI file: ' + str(f) + ', Skipping file...')
        if exitOnError:
            sys.exit()
        parsedData = [fName, '','','','','']
        return parsedData
    root = tree.getroot()
    sofaStringText_list = []
    for child in root:
        s1 = child.tag
        s2 = child.attrib
        m2 = re.search('Sofa', s1)
        if m2:
            objText = s2['sofaString']
            parsedList = objText.split(',')
            for k,v in cDict.items():
                if v == -1:
                    parsedData.append('SKIPPED')
                try:
                    s = parsedList[v]
                    s = s.rstrip('\r\n')
                    parsedData.append(s)
                except IndexError:
                    if debugMode != 'quiet':
                        print('Unable to load and parse ' + str(f) + ' as XMI file. Please check the file before proceeding. Exiting now.')
                    if exitOnError:
                        sys.exit()
                    parsedData = [fName,'','','','']
                    break
            break
    return parsedData

# main workflow function
def mapData(f, cDict, mmethod="csv", wdirName=None, debugMode='quiet', exitOnError=False):
    parsedData = []
    fName = f.split('/')[-1]
    fName = fName.split('.')[0]
    if mmethod == "csv":
        df = None
        l = []
        try:
            df = pd.read_csv(f)
            if len(df.index) == 0:
                if debugMode != 'quiet':
                    print('Pandas loaded the entire file ' + str(f) + ' into the dataframe header. Attempting alternative parsing...')
                parsedData = fallbackCSV(f, cDict)
                return (fName, parsedData)
            else:
                l = df.iloc[0].tolist()
                for k,v in cDict.items():
                    if v == -1:
                        parsedData.append('SKIPPED')
                        continue
                    try:
                        val = l[v]
                        parsedData.append(val)
                    except IndexError:
                        if debugMode != 'quiet':
                            print('Invalid provided index when parsing first row of pandas dataframe for file ' + str(f) + ', now attempting to load raw text instead.')
                        parsedData = []
                        parsedData = fallbackCSV(f, cDict, exitOnError=exitOnError)
                        break
                return (fName, parsedData)
        except pd.errors.ParserError:
            if debugMode != 'quiet':
                print('Unable to load file ' + str(f) + ' using pandas, attempting to load raw text.')
            parsedData = fallbackCSV(f, cDict, exitOnError=exitOnError)
            return (fName, parsedData)
    else:
        parsedData = importXMIFile(f, cDict, wdirName, debugMode=debugMode)
        return (fName, parsedData)

def main(parseMode, headerFile, debugMode, exitOnError, forceImport, inputDirectory, outputFile, previewMode):
    keyList = ['FileName', 'DocType', 'PatID', 'EncID', 'TS']
    # first, check header file
    hFields = importHeaderFields(headerFile, keyList)
    if hFields is None:
        sys.exit()

    # then, get list of files
    fileList = glob.glob(inputDirectory + '/*')
    fList = []
    for f in fileList:
        checkFile = Path(f)
        if not checkFile.is_file():
            print('WARNING! It does not look like the file ' + str(i) + '\nexists, or it does not have the correct PATH.')
        fList.append(f)

    # next, create temp work directory with unique name
    # this is primarily used with XMI files
    t = datetime.now()
    ts_string = t.strftime('%m%d%Y_%H%M%S')
    wdirName = 'workdir_' + ts_string
    Path(wdirName).mkdir(parents=True, exist_ok=True)

    # finally, process the files
    resultList = []
    for f in tqdm(fList):
        importMethod = checkImportFile(f=f,
                                       importMethod=parseMode,
                                       forceImport=forceImport,
                                       headerDict=hFields,
                                       wdirName=wdirName,
                                       debugMode=debugMode)
        if importMethod == "csv":
            parsedResults = mapData(f, hFields, mmethod="csv", wdirName=wdirName, debugMode=debugMode, exitOnError=exitOnError)
            resultList.append(parsedResults)
        else:
            parsedResults = mapData(f, hFields, mmethod="xml", wdirName=wdirName, debugMode=debugMode, exitOnError=exitOnError)
            resultList.append(parsedResults)
        if previewMode:
            print('PREVIEW MODE displaying only a preview of the data. The output file will only have a single line of output.\n')
            print(resultList)
            break
    if debugMode == 'quiet':
        shutil.rmtree(wdirName)
    # create combined dataframe and write to disk
    df_final_list = []
    for itm in resultList:
        k = itm[0]
        cols = itm[1]
        result = None
        col0 = [cols[0]]
        if cols[0] == 'SKIPPED':
            col0 = [str(k)]
        col1 = [cols[1]]
        col2 = [cols[2]]
        col3 = [cols[3]]
        col4 = [cols[4]]
        df1 = pd.DataFrame({
            'FileID' : col0,
            'NoteType' : col1,
            'PatientID' : col2,
            'EncounterID' : col3,
            'TimeStamp' : col4
        })
        df_final_list.append(df1)
    df_output = pd.concat(df_final_list)
    df_output.to_csv(outputFile, index=False)

if __name__ == '__main__':
    # setup
    parser = argparse.ArgumentParser()
    parser.add_argument('--parseMode', '-m', choices=['csv', 'xml'], help="parser mode for input XMI or note files", required=True)
    parser.add_argument('--headerFile', type=str, help="text file with mapping for indices in the input XMI or medical note files", required=True)
    parser.add_argument('--debugMode', '-v', choices=['verbose', 'quiet'], help="enter verbose to have verbose output and not delete temp work directories, or quiet for quiet mode", const='quiet', nargs='?', default='quiet')
    parser.add_argument('--exitOnError', choices=['True', 'False'], nargs='?', const='False', default='False', help="after setup, stop if encounter a parse error.")
    parser.add_argument('--forceImport', type=str, choices=['True', 'False'], help="If set to True, this bypasses the usual file checks when running the metadata extractor", nargs='?', const='False', default='False')
    parser.add_argument('--inputDirectory', '-i', type=str, help='Input directory name that contains XMI or medical note files', required=True)
    parser.add_argument('--outputFile', '-o', type=str, help="output file name", required=True)
    parser.add_argument('--previewMode', '-p', choices=['True', 'False'], nargs='?', const='False', default='False', help='If set to true, the workflow will parse one entry, display the output on the screen, and then exit.')
    args = parser.parse_args()

    # additional setup steps
    forceImport = False
    if args.forceImport == 'True':
        forceImport = True

    exitOnError = False
    if args.exitOnError == 'True':
        exitOnError = True

    previewMode = False
    if args.previewMode == 'True':
        previewMode = True


    # helper checks
    checkInput = Path(args.inputDirectory)
    if checkInput.is_dir():
        if not os.listdir(args.inputDirectory):
            print('No files were found in the directory: ' + str(args.inputDirectory) + ', please check the directory before proceeding.')
            sys.exit()
    else:
        print('The provided input directory ' + str(args.inputDirectory) + ' is not a directory. Please check the spelling and PATH and try again.')
        sys.exit()

    main(
      parseMode = args.parseMode,
      headerFile = args.headerFile,
      debugMode = args.debugMode,
      exitOnError = exitOnError,
      forceImport = forceImport,
      inputDirectory = args.inputDirectory,
      outputFile = args.outputFile,
      previewMode = previewMode
      )
