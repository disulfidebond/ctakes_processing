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
t = datetime.now()
ts_string = t.strftime('%m%d%Y_%H%M%S')
import csv
import statistics
import sys



# functions

## File decompression, copying, deletion
def deleteFiles(outDirPath):
  for p in Path(outDirPath).glob("*.xmi"):
    p.unlink()

def importFileFromList(f):
  fileList = []
  with open(f) as fOpen:
    for i in fOpen:
      i = i.rstrip('\r\n')
      fileList.append(i)
  return fileList

def decompressAndCopy(l, outDirPath):
  xmiList = []
  for i in l:
    outName = i.split('.')
    outName = '.'.join(outName[:-1])
    # assumes only one nested directory
    outNamePath = outName.split('/')
    outPath = ''
    try:
      outPath = outDirPath + outNamePath[1]
    except IndexError:
      print('Index Error with:')
      print('item:')
      print(i)
      print('and parsed value')
      print(outNamePath)
      print('check to make sure that the input XMI list has the full path and not just the filename')
      sys.exit()
    xmiList.append(outPath)
    with gzip.open(i, 'rb') as f_in:
      with open(outPath, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
  return xmiList

## flatfile generation
def dictFromXMLtags(f, customDict=None):
  tree = None
  try:
    tree = ET.parse(f)
  except ET.ParseError:
    return None
  except FileNotFoundError:
    return None
  root = tree.getroot()
  cui_list = []
  preferred_list = []
  codingSchemeList = []
  sofaStringText_list = []
  for child in root:
    s1 = child.tag
    s2 = child.attrib
    m = re.search('Umls', s1)
    if m:
      prefText = s2['preferredText']
      if not prefText:
        prefText = '_'
      cui_list.append(s2['cui'])
      preferred_list.append(prefText)
      codingSchemeList.append(s2['codingScheme'])
    m2 = re.search('Sofa', s1)
    if m2:
      idx_FileName = 0
      idx_DocType = 0
      idx_PatID = 0
      idx_EncID = 0
      idx_TS = 0
      # NoteIdx == idx 9
      if customDict is None:
        idx_FileName = 14
        idx_DocType = 16
        idx_PatID = 12
        idx_EncID = 13
        idx_TS = 15
      else:
        idx_FileName = int(customDict['FileName'])
        idx_DocType = int(customDict['DocType'])
        idx_PatID = int(customDict['PatID'])
        idx_EncID = int(customDict['EncID'])
        idx_TS = int(customDict['TS'])
      objTextList = []
      objText = s2['sofaString']
      if not objText:
        objTextList.append(['NA','NA','NA','NA','NA'])
      else:
        parsedList = objText.split(',')
        objText_FileName = ''
        objText_DocID = ''
        objText_DocType = ''
        objText_PatID = ''
        objText_EncID = ''
        objText_TS = ''
        try:
          # custom modification for radiology notes
          '''
          objText_parsed = parsedList[idx_FileName]
          objText_parsed1 = objText_parsed.split('\n')
          objText_parsed1 = str(objText_parsed1[1])
          objText_FileName = str(objText_parsed1) + '.csv'
          objText_DocID = objText_parsed1
          objText_PatID = objText_parsed1
          objText_EncID = objText_parsed1
          objText_tmp = ' '.join(parsedList[9:11])
          objText_DocType = str(objText_tmp)
          '''
          objText_DocType = str(parsedList[idx_DocType])
          # custom modification 03312022
          objText_FileName = str(parsedList[idx_FileName])
          tmpStr = objText_FileName.split('\n')
          tmpStr = tmpStr[1]
          objText_FileName + '.csv'
          objText_DocID = tmpStr
          # objText_FileName = str(parsedList[idx_FileName]) + '.csv' 
          # objText_DocID = str(parsedList[idx_FileName])
          # end custom modification 03312022
          objText_PatID = str(parsedList[idx_PatID])
          objText_EncID = str(parsedList[idx_EncID])
          objText_TS = str(parsedList[idx_TS])
          # end custom modification code
        except IndexError:
          # debugging
          print(customDict)
          print(parsedList)
          sys.exit()
          objText_FileName = 'PARSER_ERROR'
          objText_DocID = 'PARSER_ERROR'
          objText_DocType = 'PARSER_ERROR'
          objText_PatID = 'PARSER_ERROR'
          objText_EncID = 'PARSER_ERROR'
          objText_TS = 'PARSER_ERROR'
        objTextList = [objText_DocID,objText_DocType,objText_PatID,objText_EncID,objText_TS]
      sofaStringText_list.append(objTextList)
  return (cui_list, preferred_list, codingSchemeList, sofaStringText_list)

def createXMIList(f, customFileName=False, customDelimiters=[]):
  # customDelimiters must be a list of only two integers
  # of start and stop indices that show where to join the string
  xmiList = []
  xmiStringList = []
  # create list of files to filter from XMI list
  dedup_list = list(set(f))
  for i in dedup_list:
    xmiStringList.append(i)
    iSplit = i.split('/')
    fName = ''
    if not customFileName:
      fName_list = iSplit[1].split('.')
      fName = str(fName_list[0]) + '.csv'
    else:
      fName_list = iSplit[1].split('.')
      startIdx = int(customDelimiters[0])
      stopIdx = int(customDelimiters[1])
      fName = '.'.join(fName_list[startIdx:stopIdx])
      fName = str(fName) + '.csv'
    xmiList.append(fName)
  return (xmiList, xmiStringList)

def createFileList(f):
  # import fileList as tuple
  fileList = []
  with open(args.fileList) as fOpen:
    for i in fOpen:
      i = i.rstrip('\r\n')
      iSplit = i.split(',')
      fpath = iSplit[1]
      fnameList = iSplit[0].split('.')
      fname = str(fnameList[0]) + '.csv'
      fileList.append((fname, fpath))
  fileList = list(set(fileList))
  return fileList

def createNameTuple(df_XMI, workDirPath, useCustomDict=False, customDict=None):
  l_name = df_XMI['name'].tolist()
  l_xmi	= df_XMI['xminame'].tolist()
  l_final = list(zip(l_name, l_xmi))

  nameTuple = []
  
  ######
  print('processing data for flatfile format')
  
  for f in tqdm(l_final):
    k = f[0]
    xmiPath = str(workDirPath) + '/' + str(k) + '.xmi'
    # xmiPath = f[1]
    d = None
    if not useCustomDict:
      d = dictFromXMLtags(xmiPath)
    else:
      d = dictFromXMLtags(xmiPath, customDict=customDict)
    if d is None:
      errString = 'XMLParseError'
      nameTuple.append((k, ('NULL', 'NULL', 'NULL', 'NULL', 'NULL'), 'NULL', 'NULL', errString))
      print('error parsing:')
      print(f)
      print(xmiPath)
      # sys.exit()
      continue
    k_list = d[0]
    k_list_preferred = d[1]
    k_list_cd = d[2]
    v = d[3][0]
    # custom modification for radiology notes
    tmp_docID = k.split('.')
    tmp_docID = '.'.join(tmp_docID[0:2])
    tmp_docID = str(tmp_docID)
    v[0] = tmp_docID
    # end custom modification code
    v = tuple(v)
    for idx in range(len(k_list)):
      itm = k_list[idx]
      itm_pref = k_list_preferred[idx]
      itm_cd = k_list_cd[idx]
      nameTuple.append((k, v, itm_cd, itm, itm_pref))
  return nameTuple

def writeOutputFile(outputFileName, nameTuple_removedDups, skip_header=True):
  print('writing to output')
  with open(outputFileName, 'a') as fWrite:
    # FileName,NOTE_ID,NOTE_TYPE,PSEUDO_PAT_ID,PSEUDO_PAT_ENC_CSN_ID,Size
    if skip_header == False:
      fWrite.write('FileName||Document_ID||Document_Type||Patient_ID||Encounter_ID||Code_Domain||CUI||Preferred_Text||Document_TimeStamp||Polarity\n')
    for itm in nameTuple_removedDups:
      nm = itm[0]
      csv_itemList = list(itm[1])
      csv_ts = csv_itemList[-1]
      csv_items = '||'.join(csv_itemList[:-1])
      xmi_cd = itm[2]
      xmi_cui = None
      try:
        xmi_cui = itm[3]
      except IndexError:
        # print('IndexError at')
        # print(itm)
        # break
        continue
      xmi_pref = itm[4]
      xmi_pol = ''
      s = str(nm) + '||' + csv_items + '||' + str(xmi_cd) + '||' + str(xmi_cui) + '||' + str(xmi_pref) + '||' + str(csv_ts) + '||' + str(xmi_pol)
      fWrite.write(s + '\n')

def check_headerIdxFile(f, keyList):
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
          print('\nPlease fix this problem before running the flatfile generator.\nExiting now...')
          return None
  except FileNotFoundError:
    print('Error, unable to locate the provided file:\n')
    print(f)
    print('\nPlease ensure that it is in the current working directory,\nand that the filename is correct.')
    print('Exiting now...')
    return None
  return d

def main(fNames, workDirPath, outputFileName, headerIdxList):
  customDict = {'FileName' : 16, 'DocType' : 18, 'PatID' : 14, 'EncID' : 15, 'TS' : 17}
  # modify customDictKeys as needed
  customDictKeys = ['FileName', 'DocType', 'PatID', 'EncID', 'TS']
  if headerIdxList is None:
    print('WARNING! No index list provided. This is not recommended!')
    print('Assuming the following column labels and indices:')
    [print(k,v) for k,v in customDict.items()]
    time.sleep(2)
  else:
    returnedValue = check_headerIdxFile(headerIdxList, customDictKeys)
    if returnedValue is None:
      sys.exit()
    else:
      customDict = returnedValue
  fileNameList = []
  workDirPath = workDirPath + '/'
  # start: import list of XMI file lists as fNameList
  with open(fNames) as fOpen:
    for i in fOpen:
      i = i.rstrip('\r\n')
      fileNameList.append(i)
  # iterate over each XMI file list
  startBool = True
  for x in fileNameList:
    l = importFileFromList(x)
    # copy compressed XMI files in list to workdir specified
    xmiListName = decompressAndCopy(l,workDirPath)
    xmiList, xmiStringList = createXMIList(xmiListName)
    df_XMI = pd.DataFrame({'name' : xmiList, 'xminame' : xmiStringList})
    # create tuple of XMI and CSV, and customDict 
    nameTuple = createNameTuple(df_XMI, workDirPath, useCustomDict=True, customDict=customDict)
    print("processed " + str(x))
    nameTuple.sort()
    if startBool:
      writeOutputFile(outputFileName, nameTuple, skip_header=False)
    else:
      writeOutputFile(outputFileName, nameTuple, skip_header=True)
    deleteFiles(workDirPath)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--xmiList', '-x', help='text file with list of sets of XMI files to parse', required=True)
  parser.add_argument('--workdir', '-d', help='directory where compressed XMI files will be copied', required=True)
  parser.add_argument('--out', '-o', help='output flatfile name', required=True)
  parser.add_argument('--headerIdx', '-c', help='indices for header columns of text as comma-separated values')
  args = parser.parse_args()
  main(args.xmiList, args.workdir, args.out, args.headerIdx)

