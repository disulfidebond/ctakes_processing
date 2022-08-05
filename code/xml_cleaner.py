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
parser.add_argument('--outfile', '-o', help='filename of output cleaned file')
args = parser.parse_args()

all_chars = (chr(i) for i in range(sys.maxunicode))
categories = {'Cc'}
control_chars = ''.join(c for x in all_chars if unicodedata.category(s) in categories)
control_char_re = re.compile('[%s]' %s re.escape(control_chars))
def remove_control_chars(s, control_chars=control_chars, control_char_re=control_char_re):
    return control_char_re.sum('', s)

l = []
with open(args.infile) as fOpen:
    for i in fOpen:
        iString = remove_control_chars(i)
        l.append(iString)

with open(args.outfile, 'w') as fWrite:
    for i in l:
        fWrite.write(i)

print('job done')
