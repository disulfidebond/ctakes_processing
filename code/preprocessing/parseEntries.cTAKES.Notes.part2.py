
import pandas as pd
import string, os, math
import numpy as np
from tqdm import tqdm
import random
from datetime import datetime
from collections import Counter
import time
from pathlib import Path
import argparse
t = datetime.now()
ts_string = t.strftime('%m%d%Y_%H%M%S')

parser = argparse.ArgumentParser()
parser.add_argument('--infile', '-i', help='filename for input CSV notes file to parse')
parser.add_argument('--outdir', '-d', help='name for output directory')
parser.add_argument('--outfile', '-f', help='filename for output file', required=True)
args = parser.parse_args()

'''
Instructions:
1. Modify NOTES_CSV for the notes file to be parsed,
and OUTDIRPARENT for the location of the root directory
where folders of output files will be created. Or use argparse.
2. Specify the filename for the output file using argparse
3. If you want the output directory to be in a 
different location than the 
current working directory (not recommended),
then change cwd to that location. Note that you will also need to
update OUTDIRPARENT accordingly.
4. Run 'python parseEntries.cTAKES.NOTES.v4.benchmark.py' from commandline
'''
 
NOTES_CSV = ''
if args.infile:
  NOTES_CSV = args.infile
OUTDIRPARENT = 'ctakes_sb_userInstall_customDictionary/ctakes_4.0.0.1/'
if args.outdir:
  OUTDIRPARENT = args.outdir
cwd = os.getcwd()

print('importing data')
frame_data = []
for df_chunk in tqdm(pd.read_csv(NOTES_CSV, chunksize=100000, error_bad_lines=False, engine='python')):
  frame_data.append(df_chunk)
frame = pd.concat(frame_data)
# df = frame.loc[:,['PSEUDO_PAT_ID', 'PSEUDO_PAT_ENC_CSN_ID', 'NOTE_ID', 'NOTE_LAST_FILE_TIME', 'NOTE_TYPE', 'NOTE_TEXT']]
# PSEUDO_PAT_ID,PSEUDO_PAT_ENC_CSN_ID,NOTE_ID,NOTE_LAST_FILE_TIME,NOTE_TYPE,NOTE_TEXT,NOTE_LINE
df = frame.copy()
print('processing data')
# drop NAN values
df = df.loc[df['PSEUDO_PAT_ID'].notna(),]
df = df.loc[df['NOTE_ID'].notna(),]
df = df.loc[df['NOTE_TYPE'].notna(),]

note_id = df['NOTE_ID'].tolist()
note_id = list(set(note_id))
note_id.sort()

OUTDIR = OUTDIRPARENT + 'tempDir' + '/'
ct = 0
ctt = 0
df_notes = []
for n in note_id:
  nInt = int(n)
  df_tmp = df.loc[df['NOTE_ID'] == nInt].copy()
  df_tmp.loc[:, 'NoteIdx'] = ct
  ct += 1
  note_types = df_tmp['NOTE_TYPE'].tolist()
  note_types = list(set(note_types))
  for t in note_types:
    df_out = df_tmp.loc[df_tmp['NOTE_TYPE'] == t].copy()
    df_out = df_out.reset_index(drop=True)
    df_out.loc[:, 'TypeIdx'] = ctt
    df_notes.append(df_out)
    ctt += 1
print('merging output')
df_output = pd.concat(df_notes)
df_output = df_output.reset_index(drop=True)
df_output.to_csv(args.outfile)
print('job done')

