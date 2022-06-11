
import sys
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
import tarfile

from shutil import copyfile

parser = argparse.ArgumentParser()
parser.add_argument('--infile', '-i', help='fileSizeList file name', required=True)
parser.add_argument('--dir', '-d', help='directory where files are located', required=True)
args = parser.parse_args()

outFile_path = os.getcwd()
fileDirName = outFile_path + '/' + str(args.dir)

# read in filename,size
df = pd.read_csv(args.infile, header=None)
fileNames = df.iloc[:,0].tolist()
fileSizes = df.iloc[:,1].tolist()

# debug line
try:
  tmp = [int(x) for x in fileSizes]
except ValueError:
  print('bad int value in size column,\ncheck to make sure there is no header')
  sys.exit()

fName = str(args.infile).split('.')
fName = fName[:-1]
fName = '_'.join(fName)
# Path(outFileCsvName_path).mkdir(parents=True, exist_ok=True)
smFileDirName = outFile_path +	'/' + str(fName) + '_smFileSize'
smFileDirName_parent = str(fName) + '_smFileSize'
medFileDirName = outFile_path + '/' + str(fName) + '_medFileSize'
medFileDirName_parent = str(fName) + '_medFileSize'
lgFileDirName = outFile_path +	'/' + str(fName) + '_lgFileSize'
lgFileDirName_parent = str(fName) + '_lgFileSize'
Path(smFileDirName).mkdir(parents=True, exist_ok=True)
Path(medFileDirName).mkdir(parents=True, exist_ok=True)
Path(lgFileDirName).mkdir(parents=True, exist_ok=True)

lowSizeList = []
medSizeList = []
lgSizeList = []

for i in range(0, len(fileNames)):
  fileSize = fileSizes[i]
  sourceFileNameAndPath = fileDirName + '/' + fileNames[i]
  if fileSize <= 9999:
    lowSizeList.append(fileNames[i])
    # destFileNameAndPath   = smFileDirName + '/' + fileNames[i]
    # copyfile(sourceFileNameAndPath, destFileNameAndPath)
  elif fileSize > 9999 and fileSize < 20000:
    # destFileNameAndPath   = medFileDirName + '/' + fileNames[i]
    # copyfile(sourceFileNameAndPath, destFileNameAndPath)
    medSizeList.append(fileNames[i])
  else:
    # destFileNameAndPath   = lgFileDirName + '/' + fileNames[i]
    # copyfile(sourceFileNameAndPath, destFileNameAndPath)
    lgSizeList.append(fileNames[i])

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
    if ct == 25000:
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

fileDirName_targz = fileDirName + '.tar.gz'
print('creating tar gz compressed file from parent directory')
with tarfile.open(fileDirName_targz, "w:gz") as tar:
  tar.add(fileDirName, arcname=os.path.basename(fileDirName))

print('Jobs Done')
