
import pandas as pd
import string, os, math
from os import listdir
import numpy as np
from tqdm import tqdm
import random
from datetime import datetime
from collections import Counter
import time
from pathlib import Path
import argparse
import re
import sys
from os import listdir
import csv
import statistics
import tarfile
from shutil import copyfile

t = datetime.now()
ts_string = t.strftime('%m%d%Y_%H%M%S')

parser = argparse.ArgumentParser()
parser.add_argument('--infile', '-i', help='filename for input CSV notes file to parse', required=True)
parser.add_argument('--types', '-t', help='list of note types')
args = parser.parse_args()

NOTES_CSV = 'ctakes_4.0.0.1/Churpek_NOTE.df_split1.csv'
NOTE_TYPES='ctakes_4.0.0.1/note_type_list.txt'
if args.infile:
  NOTES_CSV = args.infile
if args.types:
  NOTE_TYPES = args.types
else:
  print('Using default note_type list')

# create new directory for results, then store as variable to prepend to outFileCsvName
outFileCsvName_path = os.getcwd()
outPathDir = NOTES_CSV.split('.')
outPathDirName = '_'.join(outPathDir[:-1])
fileSizeName = outFileCsvName_path + '_fileSizeList.txt'
outFileCsvName_path = outFileCsvName_path + '/' + outPathDirName + '/'
Path(outFileCsvName_path).mkdir(parents=True, exist_ok=True)


df_note_type_list = pd.read_csv(NOTE_TYPES)
note_type_list = df_note_type_list['TYPE'].tolist()
note_type_list = list(set(note_type_list))
note_type_list = [str(x) for x in note_type_list]

print('importing data')
frame_data = []


# read in input file
for df_chunk in tqdm(pd.read_csv(NOTES_CSV, chunksize=100000, error_bad_lines=False, engine='python')):
  frame_data.append(df_chunk)
frame = pd.concat(frame_data)

df = frame.copy()
print('processing data')

note_type_values = [[] for x in range(0, len(note_type_list))]
note_type_dict = dict(zip(note_type_list, note_type_values))

# parsing functions
def dupEntryCheck(l):
  # returns True if duplicated entries found
  # returns False if no duplicated entries found 
  if len(list(set(l))) != len(l):
    return True
  else:
    return False


def checkForPreliminaryResult(l):
  m_compile = re.compile('COMPILE STRING')
  checkedResult = []
  try:
    checkedResult = [x for x,y in enumerate(l) if re.search(m_compile, y)]
  except TypeError:
    return None
  return checkedResult

def getLen(l):
  tLenList = []
  for a in l:
    try:
      tLenList.append(len(a))
    except TypeError:
      tLenList.append(0)
  return tLenList

# filtering and analysis
note_id = df['NOTE_ID'].tolist()
note_id = list(set(note_id))
note_id.sort()
print('Found ', str(len(note_id)), ' note entries.')

ct_failedParse_A = 0
ct_failedParse_B = 0
ct_failedParse_C = 0
ct_failedParse_D = 0
ct_failedParse_E = 0
ct_failedParse_F = 0
text_length_list = []

note_ct = 0
for n in note_id:
  nInt = int(n)
  df_tmp = df.loc[df['NOTE_ID'] == nInt].copy()
  df_note_type_key = df_tmp['NOTE_TYPE'].tolist()
  df_note_type_key = list(set(df_note_type_key))
  note_type_key = str(df_note_type_key[0])
  df_tmp = df_tmp.reset_index(drop=True)
  noteID_string = str(nInt)
  outFileCsvName = noteID_string + '.csv'
  outFileCsvName = outFileCsvName_path + outFileCsvName
  textCol = df_tmp['NOTE_TEXT'].tolist()
  # handles nan and other values
  if note_type_key not in note_type_dict.keys():
    note_type_key = 'UNKNOWN'
  if len(df_tmp.index) == 1:
    # single entry, output as csv
    df_tmp.to_csv(outFileCsvName, index=False)
    note_ct += 1
    tLen = getLen(textCol)
    tmp = note_type_dict[note_type_key]
    tmp.extend(tLen)
    note_type_dict[note_type_key] = tmp
    text_length_list.extend(tLen)
  elif len(df_tmp.index) == 2:
    # check for duplicates, otherwise concat and return
    rowLines = df_tmp['NOTE_LINE'].tolist()
    if rowLines[0] == rowLines[1]:
      # rows have same NOTE_LINE number
      checkedResult = checkForPreliminaryResult(textCol)
      if not checkedResult:
        # neither text result is a preliminary report
        # only remaining step is to ensure the entries are in the correct order
        if checkedResult is None or len(checkedResult) == 0:
          df_tmp.to_csv(outFileCsvName, index=False)
          note_ct += 1
          # tLen = [len(a) for a in textCol]
          tLen = getLen(textCol)
          tmp = note_type_dict[note_type_key]
          tmp.extend(tLen)
          ct_failedParse_A += 1
          note_type_dict[note_type_key] = tmp
          text_length_list.extend(tLen)
        elif rowLines[0] == 1:
          df_out1 = df_tmp.iloc[[0]]
          df_out2 = df_tmp.iloc[[1]]
          df_out = pd.concat([df_out1, df_out2])
          df_out.to_csv(outFileCsvName, index=False)
          note_ct += 1
          # tLen = [len(a) for a in textCol]
          tLen = getLen(textCol)
          tmp = note_type_dict[note_type_key]
          tmp.extend(tLen)
          note_type_dict[note_type_key] = tmp
          text_length_list.extend(tLen)
        else:
          df_out1 = df_tmp.iloc[[1]]
          df_out2 = df_tmp.iloc[[0]]
          df_out = pd.concat([df_out1, df_out2])
          df_out.to_csv(outFileCsvName, index=False)
          note_ct += 1
          # tLen = [len(a) for a in textCol]
          tLen = getLen(textCol)
          tmp = note_type_dict[note_type_key]
          tmp.extend(tLen)
          note_type_dict[note_type_key] = tmp
          text_length_list.extend(tLen)
      else:
        # the returned result is the index to remove
        if len(checkedResult) == 2:
          # Failsafe A: both are preliminary report, just report both as a single csv file
          df_tmp.to_csv(outFileCsvName, index=False)
          note_ct += 1
          # tLen = [len(a) for a in textCol]
          tLen = getLen(textCol)
          tmp = note_type_dict[note_type_key]
          tmp.extend(tLen)
          ct_failedParse_A += 1
          note_type_dict[note_type_key] = tmp
          text_length_list.extend(tLen)
        else:
          dropIdx = checkedResult[0]
          df_out = df_tmp.drop([dropIdx])
          df_out.to_csv(outFileCsvName, index=False)
          note_ct += 1
          # tLen = [len(a) for a in textCol]
          tLen = getLen(textCol)
          tmp =	note_type_dict[note_type_key]
          tmp.extend(tLen)
          note_type_dict[note_type_key] = tmp
          text_length_list.extend(tLen)
    else:
      # rows have different NOTE_LINE number
      # checking for preliminary report status is unnecessary
      if rowLines[0] == 1:
        df_tmp.to_csv(outFileCsvName, index=False)
        note_ct += 1
        # tLen = [len(a) for a in textCol]
        tLen = getLen(textCol)
        tmp = note_type_dict[note_type_key]
        tmp.extend(tLen)
        note_type_dict[note_type_key] = tmp
        text_length_list.extend(tLen)
      else:
        df_out1 = df_tmp.iloc[[1]]
        df_out2 = df_tmp.iloc[[0]]
        df_out = pd.concat([df_out1, df_out2])
        df_out.to_csv(outFileCsvName, index=False)
        note_ct += 1
        # tLen = [len(a) for a in textCol]
        tLen = getLen(textCol)
        tmp = note_type_dict[note_type_key]
        tmp.extend(tLen)
        note_type_dict[note_type_key] = tmp
        text_length_list.extend(tLen)
  elif dupEntryCheck(textCol) == False:
    # no duplicated entries, only ordering is needed
    # also catches non-duplicated entries that would fail checkForPreliminaryResult()
    rowLines = df_tmp['NOTE_LINE'].tolist()
    tupleList = [(x,y) for x,y in enumerate(rowLines)]
    tupleList.sort(key=lambda x: x[1])
    vIdx = [x[0] for x in tupleList]
    df_reordered = df_tmp.reindex(vIdx)
    df_out = df_reordered.reset_index(drop=True)
    df_out.to_csv(outFileCsvName, index=False)
    note_ct += 1
    # tLen = [len(a) for a in textCol]
    tLen = getLen(textCol)
    tmp = note_type_dict[note_type_key]
    tmp.extend(tLen)
    note_type_dict[note_type_key] = tmp
    text_length_list.extend(tLen)
  else:
    checkedResult = checkForPreliminaryResult(textCol)
    if not checkedResult:
      if checkedResult is None:
        df_tmp.to_csv(outFileCsvName, index=False)
        note_ct += 1
        # tLen = [len(a) for a in textCol]
        tLen = getLen(textCol)
        tmp = note_type_dict[note_type_key]
        tmp.extend(tLen)
        ct_failedParse_A += 1
        note_type_dict[note_type_key] = tmp
        text_length_list.extend(tLen)
      else:
        # Failsafe C: duplicated entries but no preliminary result
        # order and output results as single csv
        rowLines = df_tmp['NOTE_LINE'].tolist()
        tupleList = [(x,y) for x,y in enumerate(rowLines)]
        tupleList.sort(key=lambda x: x[1])
        vIdx = [x[0] for x in tupleList]
        df_reordered = df_tmp.reindex(vIdx)
        df_out = df_reordered.reset_index(drop=True)
        df_out.to_csv(outFileCsvName, index=False)
        note_ct += 1
        # tLen = [len(a) for a in textCol]
        tLen = getLen(textCol)
        tmp = note_type_dict[note_type_key]
        tmp.extend(tLen)
        ct_failedParse_C += 1
        note_type_dict[note_type_key] = tmp
        text_length_list.extend(tLen)
    elif len(checkedResult) > 1:
      # Failsafe B: duplicated entries and more than one preliminary result
      # use same approach as Failsafe C
      rowLines = df_tmp['NOTE_LINE'].tolist()
      tupleList = [(x,y) for x,y in enumerate(rowLines)]
      tupleList.sort(key=lambda x: x[1])
      vIdx = [x[0] for x in tupleList]
      df_reordered = df_tmp.reindex(vIdx)
      df_out = df_reordered.reset_index(drop=True)
      df_out.to_csv(outFileCsvName, index=False)
      note_ct += 1
      # tLen = [len(a) for a in textCol]
      tLen = getLen(textCol)
      tmp = note_type_dict[note_type_key]
      tmp.extend(tLen)
      ct_failedParse_B += 1
      note_type_dict[note_type_key] = tmp
      text_length_list.extend(tLen)
    else:
      # locate preliminary result for NOTE_LINE 1 and remove
      df_note1 = df_tmp.loc[df_tmp['NOTE_LINE'] == 1,].copy()
      df_note1 = df_note1.drop(checkedResult)
      if len(df_note1.index) != 1:
        # Failsafe D: more than one non-prelim note, output to csv
        df_tmp.to_csv(outFileCsvName, index=False)
        note_ct += 1
        # tLen = [len(a) for a in textCol]
        tLen = getLen(textCol)
        tmp = note_type_dict[note_type_key]
        tmp.extend(tLen)
        ct_failedParse_D += 1
        note_type_dict[note_type_key] = tmp
        text_length_list.extend(tLen)
      else:
        # walk through each note line:
        # while NOTE_LINE <  max(NOTE_LINE)
        # # scan each NOTE_LINE where NOTE_LINE == current
        # # if match_regex to prev_line: remove
        # # else: keep and prev_line = current_line
        note1_text_list = df_note1['NOTE_TEXT'].tolist()
        prev_note_text = df_note1_text[0]
        df_note_count = df_tmp.loc[df_tmp['NOTE_LINE'] != 1,]
        note_count = df_note_count['NOTE_LINE'].tolist()
        max_note = max(note_count) + 1
        drop_indices = []
        df_noteText_text = df_tmp.loc[df_tmp['NOTE_LINE'] > 1,].copy()
        # Failsafe E: either of two conditions are met, signaling the algorithm failed
        # 1) no regex matches are found for range(2, max_note)
        # 2) len(df_out) != max_note
        failsafe_bool = False
        for x in range(2, max_note):
          df_current_textList = df_noteText_text.loc[df_noteText_text['NOTE_LINE'] == x,]
          noteText_tmp = df_current_textList['NOTE_TEXT'].tolist()
          text_scan = prev_note_text[-60:]
          m_comp = re.compile(text_scan)
          indices_tuple = [(x,y) for x,y in noteText_tmp if re.search(m_comp, y)]
          if len(indices_tuple) == 0:
            failsafe_bool = True
            break
          indices_list = [i[0] for i in indices_tuple]
          str_list = [i[1] for i in indices_tuple]
          drop_indices.extend(indices_list)
          keep_textList = [w for w in noteText_tmp if w not in str_list]
          if len(keep_textList) == 0:
            failsafe_bool = True
            break
          if len(keep_textList) > 1:
            failsfe_bool = True
            break
          prev_note_text = keep_textList[0]
        if failsafe_bool:
          df_tmp.to_csv(outFileCsvName, index=False)
          note_ct += 1
          # tLen = [len(a) for a in textCol]
          tLen = getLen(textCol)
          tmp = note_type_dict[note_type_key]
          tmp.extend(tLen)
          ct_failedParse_E += 1
          note_type_dict[note_type_key] = tmp
          text_length_list.extend(tLen)
        else:
          # finally, if failsafe_bool == False, reorder the lines and output as csv
          df_out = df_tmp.drop(drop_indices)
          sort_range = [r for r in range(0, note_max)]
          df_out = df_out.reset_index(drop=True)
          correct_order = df_out['NOTE_LINE'].tolist()
          correct_order_tuple = [a for a in enumerate(correct_order)]
          correct_order_tuple.sort(key=lambda x: x[1])
          corrected_order = [a[0] for a in correct_order_tuple]
          df_out = df_out.reindex(corrected_order)
          df_out.to_csv(outFileCsvName, index=False)
          note_ct += 1
          # tLen = [len(a) for a in textCol]
          tLen = getLen(textCol)
          tmp = note_type_dict[note_type_key]
          tmp.extend(tLen)
          note_type_dict[note_type_key] = tmp
          text_length_list.extend(tLen)
outCsvFileLog = NOTES_CSV + '.log'
with open(outCsvFileLog, 'w') as fWrite:
  fWrite.write('####\n')
  fWrite.write('Report\n')
  fWriteOut = 'Wrote a total of ' + str(note_ct) + ' notes.\n'
  fWrite.write(fWriteOut)
  fWriteOut = 'Unable to parse ' + str(ct_failedParse_A) + ' entries because 2/2 entries were prelim or incomplete notes.\n'
  fWrite.write(fWriteOut)
  fWriteOut = 'Unable to parse ' + str(ct_failedParse_B) + ' entries because more than 1 entry was duplicated and a prelim note.\n'
  fWrite.write(fWriteOut)
  fWriteOut = 'Unable to parse ' + str(ct_failedParse_C) + ' entries because there were duplicated entries with no prelim notes.\n'
  fWrite.write(fWriteOut)
  fWriteOut = 'Unable to parse ' + str(ct_failedParse_D) + ' entries because there were more than one non-unique non-prelim note.\n'
  fWrite.write(fWriteOut)
  fWriteOut = 'Unable to parse ' + str(ct_failedParse_E) + ' entries because the algorithm for parsing multiple notes failed.\n'
  fWrite.write(fWriteOut)
  fWrite.write('####\nFound the following counts for NOTE_TYPE:\n')
  fWrite.write('NOTE_TYPE,Count,MedianLength\n')
  for k,v in note_type_dict.items():
    medianVal = None
    try:
      medianval = statistics.median(v)
    except statistics.StatisticsError:
      medianVal = None
    fWriteOut = str(k) + ',' + str(len(v)) + ',' + str(medianVal) + '\n'
    fWrite.write(fWriteOut)

# create permanent copy of report file sizes
print('calculating output file sizes')
l_names = [f for f in listdir(outFileCsvName_path) if f.endswith('csv')]
l_sizes = [Path(str(outFileCsvName_path + '/' + x)).stat().st_size for x in l_names]
with open(fileSizeName, 'w') as fWrite:
  for i in range(0, len(l_names)):
    outString = str(l_names[i]) + ',' + str(l_sizes[i]) + '\n'
    # print(outString)
    fWrite.write(outString)


outFile_path = os.getcwd()
fileDirName = outFileCsvName_path
fName = str(args.infile).split('.')
fName = fName[:-1]
fName = '_'.join(fName)


# create permanent copy of file stats
print('creating report of file stats')
outCsvFileStats = outFileCsvName_path + '/' + outPathDirName + '.stats.csv'
df_outCsvFileStats = pd.DataFrame.from_dict(note_type_dict, orient='index').T
df_outCsvFileStats.to_csv(outCsvFileStats)

# create size-based sub directories
smFileDirName = outFile_path +  '/' + str(fName) + '_smFileSize'
smFileDirName_parent = str(fName) + '_smFileSize'
medFileDirName = outFile_path + '/' + str(fName) + '_medFileSize'
medFileDirName_parent = str(fName) + '_medFileSize'
lgFileDirName = outFile_path +  '/' + str(fName) + '_lgFileSize'
lgFileDirName_parent = str(fName) + '_lgFileSize'
Path(smFileDirName).mkdir(parents=True, exist_ok=True)
Path(medFileDirName).mkdir(parents=True, exist_ok=True)
Path(lgFileDirName).mkdir(parents=True, exist_ok=True)


fileDirName = outFileCsvName_path
lowSizeList = []
medSizeList = []
lgSizeList = []

for i in range(0, len(l_names)):
  fileSize = l_sizes[i]
  # sourceFileNameAndPath = fileDirName + '/' + l_names[i]
  if fileSize <= 9999:
    lowSizeList.append(l_names[i])
  elif fileSize > 9999 and fileSize < 20000:
    medSizeList.append(l_names[i])
  else:
    lgSizeList.append(l_names[i])

def sortAndOutput(l, parent_dest_fName, dest_fName, sourceDirName):
  # l-> list
  # sourceDirName -> string name of directory with PATH, example: fileDirName
  # parent_dest_fName -> string name of parent directory for sub directory, example: smFileDirName
  ct = -1
  c = 1
  destDirName = parent_dest_fName + '/' + dest_fName + '_' + str(c) + '/'
  Path(destDirName).mkdir(parents=True, exist_ok=True)
  for i in range(0, len(l)):
    ct += 1
    if ct == 50000:
      c += 1
      destDirName = parent_dest_fName + '/' + dest_fName + '_' + str(c) + '/'
      Path(destDirName).mkdir(parents=True, exist_ok=True)
      ct = -1
    source_fileName = sourceDirName + '/' + str(l[i])
    dest_fileName = destDirName + '/' + str(l[i])
    copyfile(source_fileName, dest_fileName)

sortAndOutput(lowSizeList, smFileDirName, smFileDirName_parent, fileDirName)
sortAndOutput(medSizeList, medFileDirName, medFileDirName_parent, fileDirName)
sortAndOutput(lgSizeList, lgFileDirName, lgFileDirName_parent, fileDirName)

print('Report')
sm_count = len(lowSizeList)
med_count = len(medSizeList)
lg_count = len(lgSizeList)
print('Found ' + str(len(df.index)) + ' total files.')
print('Found ' + str(sm_count) + ' files less than 10 kb.')
print('Found ' + str(med_count) + ' filess between 10 kb and 20 kb.')
print('Found ' + str(lg_count) + ' files larger than 20 kb.')

fileDirName_targz = fileDirName[:-1] + '.tar.gz'
print('creating tar gz compressed file from parent directory with name ' + str(fileDirName_targz))
with tarfile.open(fileDirName_targz, "w:gz") as tar:
  tar.add(fileDirName, arcname=os.path.basename(fileDirName))

print('Jobs Done')

