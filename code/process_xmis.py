#!/usr/bin/env python3

# This is a Python reimplementation of
# ctakes-misc/src/main/java/org/apache/ctakes/consumers/ExtractCuiSequences.java
# Credit: Dmitriy Dligach

import os
import pathlib
from cassis import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--xmiDir', '-i', help='name of directory containing XMI files', required=True)
parser.add_argument('--outputFile', '-o', help='output file name', required=True)
parser.add_argument('--excludeText', '-x', choices=["True", "False"], default="True", const="True", nargs='?', help='Setting this to False means the original text will be added to the output file. THIS MAY EXPOSE PHI and should not be set to False unless you know what you are doing.')
args = parser.parse_args()

excludeTextBool = True
if args.excludeText == "False":
  excludeTextBool = False


xmi_dir = str(args.xmiDir)

ident_annot_class_name = 'org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation'
umls_concept_class_name = 'org_apache_ctakes_typesystem_type_refsem_UmlsConcept'
type_system_path = 'TypeSystem.xml'

out_path = str(args.outputFile)

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


def process_xmi_file(xmi_path, type_system, out_file, excludeTextBool = True):
  """This is a Python staple"""

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
          str(end_offset)),
          text
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
  for out_tuple in out_tuples:
    out_file.write('|'.join(out_tuple) + '\n')

def main():
  """Main driver"""

  # load type system once for all files
  type_system_file = open(type_system_path, 'rb')
  type_system = load_typesystem(type_system_file)

  out_file = open(out_path, 'w')
  for file_name in os.listdir(xmi_dir):
    xmi_path = os.path.join(xmi_dir, file_name)
    process_xmi_file(xmi_path, type_system, out_file)

if __name__ == "__main__":

  main()
