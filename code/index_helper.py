import xml.etree.ElementTree as ET
import re
import argparse
import sys

# functions
def dictFromXMLtags(f, customDict=None):
    tree = None
    try:
        tree = ET.parse(f)
    except ET.ParseError:
        return None
    except FileNotFoundError:
        return None
    root = tree.getroot()
    for child in root:
        s1 = child.tag
        s2 = child.attrib
        m2 = re.search('Sofa', s1)
        if m2:
            objText = s2['sofaString']
            x = objText.split('\n')
            return objText.split('\n')

def formatOutput(l, verboseBool=False, countBool=True):
    idxCt = len(l)
    if verboseBool:
        print('Index,HeaderField,ExampleText')
    elif not countBool:
       print('Index,HeaderField')
    else:
        print('HeaderField,Index,AdjustedIndex')
    for idx,val in enumerate(l):
        s = str(val[0]) + ',' + str(idx)
        if verboseBool:
            s = str(idx) + ',' + str(val[0]) + ',' + str(val[1])
            print(s)
        elif not countBool:
            print(s)
        else:
            idxCtString = idx + idxCt
            idxCtString = str(idxCtString)
            s = s + ',' + idxCtString
            print(s)

# argparse
parser = argparse.ArgumentParser()
parser.add_argument('--xmiFile', '-x', help='xmi text file to scan', required=True)
parser.add_argument('--exampleText', '-e', type=bool, help='set to true to show example text from column', nargs='?', const=False, default=False)
parser.add_argument('--textColInt', '-t', type=int, nargs='?', const=-1, default=-1, help='index of text column')
args = parser.parse_args()


parsedList = dictFromXMLtags(args.xmiFile)
if parsedList is None:
    print('Error with parsing file')
    sys.exit()
cList = parsedList[0].split(',')
dList = parsedList[1].split(',')
tList = []
print(cList)
stopCt = len(cList)
countBool = True
if args.textColInt >=0:
    stopCt = args.textColInt
    countBool = False
for x in range(stopCt):
    tList.append((cList[x], dList[x]))

if args.exampleText == True:
    formatOutput(tList, verboseBool=True)
elif not countBool:
    formatOutput(tList, countBool=False)
else:
    formatOutput(tList)
