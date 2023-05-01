import pandas as pd
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

parser = argparse.ArgumentParser()
parser.add_argument('--infile', '-i', help='filename for input CSV notes file to clean', required=True)
parser.add_argument('--outfile', '-o', help='filename of output cleaned file', required=True)
args = parser.parse_args()

def remove_control_chars(s):
    all_chars = (chr(i) for i in range(sys.maxunicode))
    categories = {'Cc'}
    control_chars = ''.join(map(chr, itertools.chain(range(0x00,0x20),range(0x7f,0xa0))))
    try:
        control_char_re = re.compile('[%s]' % re.escape(control_chars))
        return control_char_re.sub('', s)
    except TypeError:
        return s

l = []
with open(args.infile) as fOpen:
    for i in fOpen:
        iString = remove_control_chars(i)
        l.append(iString)

with open(args.outfile, 'w') as fWrite:
    for i in l:
        fWrite.write(i+'\n')

print('job done')

