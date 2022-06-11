
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

parser = argparse.ArgumentParser()
parser.add_argument('--dir', '-d', help='directory name to scan files', required=True)
parser.add_argument('--out', '-o', help='output file name', required=True)
args = parser.parse_args()

outFileCsvName_path = os.getcwd()

if args.dir:
  outFileCsvName_path = outFileCsvName_path + '/' + args.dir

outputFileName = args.out # outFileCsvName_path + '_stat_fileSizeList.txt'
print('scanning ' + str(outFileCsvName_path))
time.sleep(1)
# create new directory for results, then store as variable to prepend to outFileCsvName
l = [f for f in listdir(outFileCsvName_path) if f.endswith('csv')]
nameDict = dict()
for f in l:
  # CHANGEME
  # kList = f.split('.')
  # k = '_'.join(kList[0:2])
  k = f
  sizeTmp = Path(str(outFileCsvName_path + '/' + f)).stat().st_size
  v = sizeTmp
  nameDict[k] = v
with open(outputFileName, 'w') as fWrite:
  # fWrite.write('FileName,Size\n')
  for k,v in nameDict.items():
    s = str(k) + ',' + str(v)
    fWrite.write(s + '\n')
print('job done')
