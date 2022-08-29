import gzip
import shutil
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

t = datetime.now()
ts_string = t.strftime('%m%d%Y_%H%M%S')

parser = argparse.ArgumentParser()
parser.add_argument('--dirName', '-d', help='name of directory with compressed XMI files', required=True)
parser.add_argument('--dirCt', '-c', help='count for number of files in working directory', nargs='?', const=10000, type=int, default=10000)
parser.add_argument('--dirLimit', '-l', help='number of simultaneous flatfile instances to allow', nargs='?', const=3, type=int, default=3)
parser.add_argument('--runLabel', '-r', type=str, help='label for run files')
args = parser.parse_args()


# functions
def writeToOutput(l, setCt, runDirName):
  fOutName = runDirName + '/' + 'runList.xmi_set.' + str(setCt) + '.txt'
  with open(fOutName, 'w') as fWrite:
    for i in l:
      fWrite.write(i + '\n')

def getFilesInDir(d):
  fNameList = []
  for (dirpath, dirnames, filenames) in os.walk(d):
    fNameList.extend(filenames)
  dirCt = len(fNameList)
  return (fNameList, dirCt)

# get list of XMI filenames
fNameList = getFilesInDir(args.dirName)
fNameList = fNameList[0]

# initial checks
# check to make sure all files are gz-compressed
checkGZ = [x for x in fNameList if not x.endswith('gz')]
if checkGZ:
  print('Warning, found files in target directory ' + str(args.dirName) + ' that were not gz-compressed.')
  print('Please check the directory before running this script. Exiting now...')
  sys.exit()

# check to make sure dirLimit is within range
if int(args.dirLimit) > 0 and int(args.dirLimit) <= 20:
  print('ready to run flatfile generation with ' + str(args.dirLimit) + ' simultaneous instances')
else:
  print('Error, the number of simultaneous flatfile instances can only be >= 1 or <= 20')
  sys.exit()

# setup
fNamePath = [(str(args.dirName) + '/' + str(x)) for x in fNameList]
runFileLabel = 'xmiList.' + ts_string + '.txt'
runDir = 'fileListDir_' + ts_string
if args.runLabel:
  runFileLabel = 'xmiList.' + str(args.runLabel) + '.' + ts_string
  runDir = str(args.runLabel) + '_' + ts_string


Path(runDir).mkdir(parents=True, exist_ok=True)

# create manifest files in runDir
ct = -1
setCt = 0
sampledList = []
stopCt = int(args.dirCt)
for i in fNamePath:
  ct += 1
  if ct == stopCt:
    writeToOutput(sampledList, setCt, runDir)
    setCt += 1
    ct = 0
    sampledList = []
    sampledList.append(i)
  else:
    sampledList.append(i)
writeToOutput(sampledList, setCt, runDir)

# make workdirs using provided limit
for w in range(1, (int(args.dirLimit)+1)):
  wDirName = 'workdir' + str(w)
  Path(wDirName).mkdir(parents=True, exist_ok=True)

# create runfiles that will be input to flatfile generator script
# args.dirLimit is a limiting parameter
fileCountInRunDir = getFilesInDir(runDir)
filesInRunDir = fileCountInRunDir[0]
fileCountInRunDir = fileCountInRunDir[1]
filesInRunDir = [((runDir) + '/' + str(x)) for x in filesInRunDir]
if int(args.dirLimit) == 1:
  runFileName = runFileLabel + '.txt'
  with open(runFileName, 'w') as fWrite:
    for i in filesInRunDir:
      fWrite.write(str(i) + '\n')
  print('created runfile for flatfile generator with prefix ' + str(runFileLabel))
elif int(args.dirLimit) == 2:
  limitRange = int(int(fileCountInRunDir)/int(args.dirLimit))
  skipWrite = False
  if fileCountInRunDir%2 == 1:
    skipWrite = True
  ct = 1
  writeList = []
  for i in range(0, fileCountInRunDir):
    if i == limitRange:
      if skipWrite:
        writeList.append(filesInRunDir[i])
      else:
        runFileName = runFileLabel + '.' + str(ct) + '.txt'
        with open(runFileName, 'w') as fWrite:
          for w in writeList:
            fWrite.write(str(w) + '\n')
        ct += 1
        limitRange = limitRange*2
        writeList = []
        writeList.append(filesInRunDir[i])
    else:
      writeList.append(filesInRunDir[i])
  if skipWrite:
    runFileName = runFileLabel + '.' + str(ct) + '.txt'
    with open(runFileName, 'w') as fWrite:
      for w in writeList:
        fWrite.write(str(w) + '\n')
  print('created runfiles for flatfile generator with prefix ' + str(runFileLabel))
else:
  limitRange = int(int(fileCountInRunDir)/int(args.dirLimit))
  ct = 1
  writeList = []
  stopCt = -1
  for i in range(0, fileCountInRunDir):
    stopCt += 1
    if stopCt == limitRange:
      print(limitRange)
      runFileName = runFileLabel + '.' + str(ct) + '.txt'
      with open(runFileName, 'w') as fWrite:
        for w in writeList:
          fWrite.write(str(w) + '\n')
      ct += 1
      # limitRange = limitRange*2
      writeList = []
      writeList.append(filesInRunDir[i])
      stopCt = 0
    else:
      writeList.append(filesInRunDir[i])
    runFileName = runFileLabel + '.' + str(ct) + '.txt'
  with open(runFileName, 'w') as fWrite:
    for w in writeList:
      fWrite.write(str(w) + '\n')
  print('created runfiles for flatfile generator with prefix ' + str(runFileLabel))
print('Setup of work directories completed')

