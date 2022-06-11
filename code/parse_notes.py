import os
import pandas as pd
from tqdm import tqdm

# MODIFY ME
# provide an input filename that contains clinical notes
NOTES_FILE=''
# provide a term that can be used to split the data into manageable groups
# for example, you could split the data by encounter ID
SPLIT_ID=''
# end MODIFY ME

frame_data = []
for df_chunk in tqdm(pd.read_csv(NOTES_FILE, chunksize=100000, error_bad_lines=False, engine='python')):
  frame_data.append(df_chunk)
df = pd.concat(frame_data)
df_len = len(df.index)
print(f'dataframe has {df_len} values')
pat_id_list = df[SPLIT_ID].to_list()
pat_id_list = list(set(pat_id_list))
pat_id_list.sort()

print('found this many unique identifiers:')
print(len(pat_id_list))

print('writing to individual output CSV files')
counter = 0
for i in tqdm(pat_id_list):
  df_tmp = df.loc[df[SPLIT_ID].isin([i])]
  # print(df_tmp)
  counter += len(df_tmp.index)
  for x in range(len(df_tmp.index)):
    idx = x+1
    fName = 'output_parse/' + str(i) + '.' + str(idx) + '.csv'
    df_tmp.iloc[[x]].to_csv(fName, index=False)
print(f'found total of {counter} values')
