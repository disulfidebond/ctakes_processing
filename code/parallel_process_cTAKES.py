import os
from datetime import datetime
import tarfile
import os.path
import gzip
import glob
from tqdm import tqdm
import shutil
import subprocess
from pathlib import Path
from multiprocessing import Pool
import time
import argparse

# setup
parser = argparse.ArgumentParser()
parser.add_argument('--inputDir', '-i', help='Name of the input directory of notes. Notes MUST end in .csv', required=True)
parser.add_argument('--splitCount', '-c', type=int, help='number of files per cTAKES instance', const=5000, default=5000, nargs='?')
parser.add_argument('--instanceLimit', '-l', type=int, help='number of concurrent cTAKES instances to run', const=10, default=10, nargs='?')
parser.add_argument('--templateDir', '-t', type=str, help='name of the directory containing cTAKES that will be copied to each worker node', required=True)
parser.add_argument('--outputDir', '-o', help='name of directory where output compressed XMI files will be copied to', required=True)
parser.add_argument('--keepWorkDirs', choices=['True', 'False'], help='if set to True, output directories from each worker will be kept. Default is False', const='False', default='False', nargs='?')
args = parser.parse_args()

'''
Requirements:
1. Modify the runCTAKES.sh script as described in the documentation, then add it to the cTAKES template directory
2. The name of the input directory of notes, all of which must end in *.csv
3. The name of the cTAKES template directory
4. The name of the output directory where all xmi.gz files from cTAKES will be copied (this is required even if keepWorkDirs is set to True)

Optional Arguments:
1. set -c to an integer > 1 to specify how many notes will be copied to each cTAKES instance (default is 5000)
2. set -l to an integer > 1 to specify the number of parallel cTAKES instances to run (default is 10), do not set this to a value higher than (total_CPU_cores/2)
'''

# setup
if not os.path.isdir(str(args.templateDir)):
    print('Error, the provided directory for --templateDir ' + str(args.templateDir) + ' does not exist, or has a typo')
    print('Please check the name and try again')
    print('Exiting now...')
    sys.exit()

keepWorkDirBool = False
if args.keepWorkDirs == 'True':
    keepWorkDirBool = True

outputDirName = 'NULL'
logDirName = 'NULL'
if not Path(args.outputDir).exists():
    outputDirName = str(args.outputDir)
else:
    tstamp = datetime.now()
    ts_string = tstamp.strftime('%m%d%Y_%H%M%S')
    # Path('inputDir').mkdir(parents=True, exist_ok=True)
    print('Error, the provided output directory name exists.\nAppending a timestamp to the provided directory name to prevent overwriting.')
    outputDirName = str(args.outputDir) + ts_string
tstamp = datetime.now()
ts_logs = tstamp.strftime('%m%d%Y_%H%M%S')
Path(outputDirName).mkdir(parents=True, exist_ok=True)    
logDirName = str(outputDirName) + '_logFiles_' + ts_logs
Path(logDirName).mkdir(parents=True, exist_ok=True)

inputDir = args.inputDir
gString = inputDir + '/*.csv'
print('gathering list of all input files at ' + str(inputDir))
inFileList = glob.glob(gString)
if len(inFileList) == 0:
    print('Error, no input files ending in .csv detected, exiting now...')
    sys.exit()
splitCount = args.splitCount # number of files per instance
instanceLimit = args.instanceLimit # 0-indexed count of simultaneous run instances



# functions
def make_tarfile(output_filename, source_dir):
    # source: https:stackoverflow.com/a/17081026
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def parallelParse(t):
    successBool = True
    keepWorkDirBool = t[4]
    outName = t[2] # outputDirName
    logDirName = t[3]
    tstamp = datetime.now()
    ts_string = tstamp.strftime('%m%d%Y_%H%M%S')
    instanceNum = t[0]
    runDirName = 'run_instance.' + ts_string + '.' + str(args.templateDir) + '_' + str(instanceNum)
    # create unique run directory
    srcDir = os.getcwd() + '/' + str(args.templateDir)
    destDir = os.getcwd() + '/' + runDirName
    shutil.copytree(srcDir, destDir)
    logName = 'ctakes_pyrunLog.' + ts_string + '.txt'
    # setup: need to copy files to unique runInstance directory
    destList = [x[1] for x in t[1]]
    destList = [(str(runDirName) + '/inputDir/' + str(x)) for x in destList]
    srcList = [x[0] for x in t[1]]
    for itm in range(0, len(destList)):
        shutil.copy(srcList[itm], destList[itm])
    rootDir = os.getcwd()
    os.chdir(runDirName)
    # add input file list to log
    with open(logName, 'a') as fWrite:
        fWrite.write('input file list for ' + ts_string + ':\n')
        for i in srcList:
            fWrite.write(i+'\n')
        fWrite.write('end of input files\n')
        fWrite.write('currently in directory ' + runDirName + '\n')
    # finished setup, ready to run cTAKES
    runDirCommand = ['./runCTAKES.sh']
    ctakesRun = subprocess.run(runDirCommand, shell=True, stdout=subprocess.PIPE)
    if ctakesRun.returncode == 0:
        with open(logName, 'a') as fWrite:
            fWrite.write('finished processing ' + str(instanceNum) + ' at timestamp ' + str(ts_string) + '\n')
        try:
            rmDirName = 'inputDir'
            shutil.rmtree(rmDirName)
        except FileNotFoundError:
            with open(logName, 'a') as fWrite:
                fWrite.write('Warning, inputDir not found after completing run ' + str(instanceNum) + ' in directory ' + str(runDirName) + '\n')
            successBool = False
        Path('inputDir').mkdir(parents=True, exist_ok=True)
        gString = 'outputDir/*.xmi'
        gList = glob.glob(gString)
        if not gList:
            successBool = False
            print('Warning, no XMI files detected at ' + str(gString))
            print('Hint: Double check that you have correctly set the API parameters!')
            with open(logName, 'a') as fWrite:
                fWrite.write('Warning, no XMI files detected at ' + str(gString) + '\n')
        else:
            for f in gList:
                fOutName = str(f) + '.gz'
                with open(f, 'rb') as f_in, gzip.open(fOutName, 'wb') as f_out:
                    f_out.writelines(f_in)
                Path(f).unlink()
            with open(logName, 'a') as fWrite:
                fWrite.write('Finished compressing and deleting output XMI files from timestamp ' + str(ts_string) + '\n')
    else:
        successBool = False
        with open(logName, 'a') as fWrite:
            runDirError = runDirName + '.' + ts_string + '.error.tar.gz'
            fWrite.write('error when processing ' + str(instanceNum) + ' at timestamp ' + str(ts_string) + '\n')
            fWrite.write('compressing IO directories of ' + str(runDirError) + ' with timestamp ' + str(ts_string) + '\n')
        renamedDir_input = 'inputDir_' + ts_string
        renamedDir_output = 'outputDir_' + ts_string
        os.rename('inputDir', renamedDir_input)
        time.sleep(2)
        os.rename('outputDir', renamedDir_output)
        time.sleep(2)
        make_tarfile(renamedDir_input+'error.tar.gz', renamedDir_input)
        make_tarfile(renamedDir_output+'error.tar.gz', renamedDir_output)
        Path('inputDir').mkdir(parents=True, exist_ok=True)
        time.sleep(2)
        Path('outputDir').mkdir(parents=True, exist_ok=True)
        time.sleep(2)
    with open(logName, 'a') as fWrite:
        fWrite.write('#####\n')
    # return to parent directory to reset
    # then copy logs and output to designated directories
    time.sleep(1)
    os.chdir(rootDir)
    logString = runDirName + '/ctakes*.txt'
    logList = glob.glob(logString)
    destList_logs = [x.split('/')[-1] for x in logList]
    destList_logs = [str(logDirName) + '/' + x for x in destList_logs]
    logTuple = list(zip(logList, destList_logs))
    for itm in logTuple:
        shutil.copy(itm[0], itm[1])
    if successBool:
        fileListString = runDirName + '/outputDir/*.xmi.gz'
        fileList = glob.glob(fileListString)
        fileList_dest = [x.split('/')[-1] for x in fileList]
        fileList_dest = [str(outName) + '/' + str(x) for x in fileList_dest]
        fileTuple = list(zip(fileList, fileList_dest))
        for itm in fileTuple:
            shutil.copy(itm[0], itm[1])
        time.sleep(1)
        if not keepWorkDirBool:
            shutil.rmtree(runDirName)
        return (True, runDirName)
    else:
        return (False, runDirName)

tupleList = []
ct = 0
idx = 0
tmpList = []
print('building list of lists of input...')
for i in tqdm(inFileList):
    if idx > splitCount: 
        idx=0
        tupleList.append((ct, tmpList, outputDirName, logDirName, keepWorkDirBool))
        ct += 1
        tmpList = []
    f = i.split('/')[-1]
    tmpList.append((i, f))
    idx += 1
tupleList.append((ct, tmpList, outputDirName, logDirName, keepWorkDirBool))

v_ct=0
for i in tupleList:
    v_ct += len(i[1])
if len(inFileList) != v_ct:
    print('Error! The counts after generating list of lists do not match, exiting now.')
    sys.exit()
else:
    print('Passed counts validation.')

errList = []
print('Now starting parallel cTAKES processing.')
with Pool(processes=instanceLimit) as p:
    maxCt = len(tupleList)
    with tqdm(total=maxCt) as pbar:
        for i,j in enumerate(p.imap_unordered(parallelParse, tupleList)):
            pbar.update()
            errList.append(j)

print('Now checking for reported errors...')
detectedErrors = [x for x in errList if x[0] == False]
if detectedErrors:
    print('WARNING! Errors were detected in the following run instances:')
    for i in detectedErrors:
        print(i)
else:
    print('No errors detected.')
print('work complete')

