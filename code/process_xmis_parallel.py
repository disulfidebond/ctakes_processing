#!/usr/bin/env python3

# This is a Python reimplementation of
# ctakes-misc/src/main/java/org/apache/ctakes/consumers/ExtractCuiSequences.java
# Credit: Dmitriy Dligach

import sys
import os
import pathlib
from cassis import *
import argparse
import shutil
import gzip
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from multiprocessing import Pool
import glob
import time

'''
Version 1.0: Parallelized implementation of pythonic workflow that uses Cassis to generate a flatfile 
Requirements: A TypeSystem.xml file must be in the same working directory as this python script
'''

# setup
parser = argparse.ArgumentParser()
parser.add_argument('--xmiDir', '-i', help='name of directory containing XMI files', required=True)
parser.add_argument('--outputFile', '-o', help='output file name', required=True)
parser.add_argument('--excludeText', '-x', choices=["True", "False"], default="True", const="True", nargs='?', help='Setting this to False means the original text will be added to the output file. THIS MAY EXPOSE PHI and should not be set to False unless you know what you are doing.')
parser.add_argument('--splitInt', '-s', type=int, help="number of files per worker", default=1000, const=1000, nargs='?')
parser.add_argument('--numWorkers', '-n', type=int, help="max number of simultaneous worker nodes to allow", default=4, const=4, nargs='?')
parser.add_argument('--outputAll', '--a', type=str, choices=["True", "False"], default="False", const="False", nargs="?", help="If set to True, then each worker node will output a flatfile in addition to the final output")
args = parser.parse_args()

excludeTextBool = True
if args.excludeText == "False":
  excludeTextBool = False

verboseOutputBool = False
if args.outputAll == "True":
  verboseOutputBool = True

xmi_dir = str(args.xmiDir)

if args.splitInt == 0:
  print('Error, split count must be greater than zero, exiting now.')
  sys.exit()

if args.splitInt < 100:
  print('Warning, setting splitInt to a value less than 100 is not recommended.')

numWorkers = args.numWorkers

ident_annot_class_name = 'org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation'
umls_concept_class_name = 'org_apache_ctakes_typesystem_type_refsem_UmlsConcept'

out_path = str(args.outputFile)

# functions
def decompressAndCopy(l, outDirPath):
  xmiList = []
  stopCt = 0
  for i in l:
      checkExists = Path(i)
      if not checkExists.is_file():
          print('Error, unable to locate input file ' + str(i))
          stopCt += 1
          if stopCt > 10:
              print('More than 10 files could not be located for decompressAndCopy step, exiting now.')
              sys.exit()
          continue
      bName = os.path.basename(i)
      outName = bName.split('.')
      outName = '.'.join(outName[:-1])
      outPath = outDirPath + '/' + outName
      xmiList.append(outPath)
      with gzip.open(i, 'rb') as f_in:
          with open(outPath, 'wb') as f_out:
              shutil.copyfileobj(f_in, f_out)
  return xmiList

def deleteFiles(outDirPath):
  for p in Path(outDirPath).glob("*.xmi"):
    p.unlink()
  os.rmdir(outDirPath)

def get_cui_coding_sceme_preferred_text(identified_annot):
  """Extract CUIs and other info from an identified annotation"""

  # same CUI often added multiple times
  # but must include it only once
  # key: cui, value = (coding scheme, pref text)
  cui_info = {}

  ontology_concept_arr = identified_annot['ontologyConceptArr']
  if not ontology_concept_arr:
    # not a umls entity, e.g. fraction annotation
    return cui_info

  # signs/symptoms, disease/disorders etc. have CUIs
  for ontology_concept in ontology_concept_arr.elements:
    if type(ontology_concept).__name__ == umls_concept_class_name:
      coding_scheme = ontology_concept['codingScheme']
      pref_text = ontology_concept['preferredText']
      cui = ontology_concept['cui']
      if cui not in cui_info.keys():
        cui_info[cui] = [[coding_scheme], [pref_text]]
      else:
        v_tmp = cui_info[cui]
        v_tmp[0].append(coding_scheme)
        v_tmp[1].append(pref_text)
        cui_info[cui] = v_tmp
    else:
      print('WARNING, encountered ontology concept that was not type refsem_UmlsConcept')
  cui_info_final = dict()
  for k,v in cui_info.items():
    cd_sch = list(set(v[0]))
    cd_sch = ','.join(cd_sch)
    pf_txt = v[1][0]
    cui_info_final[k] = (cd_sch, pf_txt)
  return cui_info_final

def process_xmi_file(xmi_path, type_system, out_file, excludeTextBool = None, verboseOutputBool = None):
  xmi_file = open(xmi_path, 'rb')
  cas = load_cas_from_xmi(xmi_file, typesystem=type_system)
  sys_view = cas.get_view('_InitialView')
  source_file_name = pathlib.Path(xmi_path).stem

  out_tuples = []
  for ident_annot in sys_view.select(ident_annot_class_name):
    text = ident_annot.get_covered_text()
    start_offset = ident_annot['begin']
    end_offset = ident_annot['end']

    cui_info = get_cui_coding_sceme_preferred_text(ident_annot)
    for cui, (coding_scheme, pref_text) in cui_info.items():
      if not excludeTextBool:
        out_tuple = (
          source_file_name,
          cui,
          coding_scheme.lower(),
          str(pref_text), # sometimes None
          str(start_offset),
          str(end_offset),
          text
        )
      else:
        out_tuple = (
          source_file_name,
          cui,
          coding_scheme.lower(),
          str(pref_text),
          str(start_offset),
          str(end_offset)
        )
      out_tuples.append(out_tuple)
  
  # output tuples sorted by start offset
  out_tuples.sort(key = lambda x: int(x[4]))
  returnList = []
  # remove this block after debugging
  if verboseOutputBool:
    with open(out_file, 'a') as fWrite:
      for out_tuple in out_tuples:
        s = '|'.join(out_tuple)
        s = s + '\n'
        fWrite.write(s)
        returnList.append(s)
  else:
    for out_tuple in out_tuples:
      s = '|'.join(out_tuple)
      s = s + '\n'
      returnList.append(s)
  return returnList

def workerNode(t):
  l = t[0]
  idxInt = t[1]
  type_system = None
  type_system_path = t[2]
  with open(type_system_path, 'rb') as fOpen:
    type_system = load_typesystem(fOpen)
  excludeTextBool = t[3]
  verboseOutputBool = t[4]
  # worker node setup
  ts = datetime.now()
  ts_string = ts.strftime('%m%d%Y_%H%M%S')
  wdir = os.getcwd()
  wDirName = wdir + '/' + 'workDir_' + ts_string + '_subset' + str(idxInt)
  Path(wDirName).mkdir(parents=True, exist_ok=True)
  out_file = 'subset_' + str(idxInt) + '.' + ts_string + '.txt'
  xmiList = decompressAndCopy(l, wDirName)
  # worker node processing
  processedCUIs = []
  for f in xmiList:
    vals = process_xmi_file(f, type_system, out_file, excludeTextBool=excludeTextBool, verboseOutputBool=verboseOutputBool)
    processedCUIs.append(vals)
  return (processedCUIs, wDirName) 

def main():
  # split into list of lists
  countInt = args.splitInt
  startInt = 0
  stepInt = countInt
  gString = str(xmi_dir) + '/*.xmi.gz'
  xmiFiles = glob.glob(gString)
  if len(xmiFiles) == 0:
    print('Error, no input files detected.\nPlease check the input directory ' + str(xmi_dir) + '\nand check that the input files are gz-compressed.\n\nExiting now...')
    sys.exit()
  totalCount = len(xmiFiles)
  incrementInt = int(totalCount/countInt) - 1
  print(countInt)
  print(incrementInt)
  split_xmi = []
  for i in range(0, incrementInt):
    tmp = xmiFiles[startInt:stepInt]
    split_xmi.append((tmp, i, 'TypeSystem.xml', excludeTextBool, verboseOutputBool))
    startInt = stepInt
    stepInt += countInt
  tmp = xmiFiles[startInt:]
  split_xmi.append((tmp, incrementInt+1, 'TypeSystem.xml', excludeTextBool, verboseOutputBool))
  print('split into ' + str(len(split_xmi)) + ' lists')

  results = []
  with Pool(processes=numWorkers) as p:
    maxCt = len(split_xmi)
    with tqdm(total=maxCt) as pbar:
      for i,j in enumerate(p.imap_unordered(workerNode, split_xmi)):
        pbar.update()
        rList = j[0]
        rDir = j[1]
        results.append(rList)
        nullVal = deleteFiles(rDir)

  with open(out_path, 'w') as fWrite:
    for sublist in results:
      for s in sublist:
        s_out = ''.join(s)
        fWrite.write(s_out)

if __name__ == "__main__":

  main()
