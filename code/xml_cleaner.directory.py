import string, os, math
import numpy as np
from datetime import datetime
from collections import Counter
import time
from pathlib import Path
import argparse
import re
import sys
from os import listdir
import csv
import unicodedata, itertools
import glob
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('--inDir', '-i', help='input directory of CSV notes to be cleaned', required=True)
parser.add_argument('--outDir', '-o', help='Name of output directory with cleaned files', required=True)
parser.add_argument('--appendSuffix', '-a', help='If empty, cleaned files will have the same filename as originals. Otherwise, cleaned files will have the provided string appended before csv', nargs='?', default='', const='')
parser.add_argument('--verbose', help='Enable verbose mode. Default is False', choices=['True', 'False'], nargs='?', default='False', const='False')
args = parser.parse_args()

# setup
verboseBool = False
if str(args.verbose) == 'True':
    verboseBool = True

if not os.path.isdir(str(args.inDir)):
    print('Error, the provided directory ' + str(args.inDir) + ' does not exist, or is not a directory.')
    print('Please check the name and try again. Exiting now...')
    sys.exit()

if os.path.isdir(str(args.outDir)):
    print('WARNING! A directory exists with the same name as the argument for --outDir')
    print('This is not recommended. If you are certain this is what you want to do,\ncontinue with the workflow, otherwise press control-c to cancel.')
    print('Pausing for 5 seconds...')
    time.sleep(5)

Path(str(args.outDir)).mkdir(parents=True, exist_ok=True)

def remove_control_chars(s):
    all_chars = (chr(i) for i in range(sys.maxunicode))
    categories = {'Cc'}
    control_chars = ''.join(map(chr, itertools.chain(range(0x00,0x20),range(0x7f,0xa0))))
    try:
        control_char_re = re.compile('[%s]' % re.escape(control_chars))
        return control_char_re.sub('', s)
    except TypeError:
        return s
# end setup

gString = str(args.inDir) + '/' + '*.csv'
fList = glob.glob(gString)

importedFiles = []
if verboseBool:
    print('importing files')
    for f in tqdm(fList):
        l = []
        fName = os.path.basename(f)
        with open(f) as fOpen:
            for i in fOpen:
                iString = remove_control_chars(i)
                l.append(iString)
        importedFiles.append((fName, l))
    print('now writing cleaned files to output')
    for t in tqdm(importedFiles):
        outFileName = t[0]
        if args.appendSuffix:
            s = t[0]
            s = '.'.join(s.split('.')[:-1])
            outFileName = s + '.' + str(args.appendSuffix) + '.csv'
        outFilePath = str(args.outDir) + '/' + outFileName
        with open(outFilePath, 'w') as fWrite:
            for i in t[1]:
                fWrite.write(i+'\n')
    if verboseBool:
        print('job done')
else:
    for f in fList:
        l = []
        fName = os.path.basename(f)
        with open(f) as fOpen:
            for i in fOpen:
                iString = remove_control_chars(i)
                l.append(iString)
        importedFiles.append((fName, l))
    for t in importedFiles:
        outFileName = t[0]
        if args.appendSuffix:
            s = t[0]
            s = '.'.join(s.split('.')[:-1])
            outFileName = s + '.' + str(args.appendSuffix) + '.csv'
        outFilePath = str(args.outDir) + '/' + outFileName
        with open(outFilePath, 'w') as fWrite:
            for i in t[1]:
                fWrite.write(i+'\n')

