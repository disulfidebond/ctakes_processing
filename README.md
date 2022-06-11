# README
Requirements:

- cTAKES 4.0.0.1 installed
- custom NLM Library installed 
- python 3.6+ installed

## Initial Comments
cTAKES processing requires an NLM library. This can be either a local custom library, or the library online. In both cases, it is required that your workflow authenticates to the NLM, even if you are using a local library. If you need to run cTAKES in an environment that is completely isolated from the Internet, then contact Sean Finan on the cTAKES dev team for assistance.

There are several versions of cTAKES available. This workflow uses cTAKES 4.0.0.1, and it has not been tested against other versions.

In addition to cTAKES, you must have Java installed, and python 3.6+ installed. Although the documentation stated cTAKES is compatible with 
Java 1.8+, we found Java 15 worked the best.

# Preprocessing
First, run [parse_notes.py](code/parse_notes.py) to split the initial CSV file. Update the `NOTES_FILE=` variable with the correct file name, and update SPLIT_ID to contain a term that can be used to split the dataset into manageable sizes.

Then, run `parseEntries.cTAKES.Notes.getFileSizes.v3.py` to generate a file list with file sizes, and run parseEntries.cTAKES.Notes.sortBySize.v3.py to sort the files by size.

# Run cTAKES
cTAKES is atomic, and by itself cannot be parallelized. However, it is possible to start multiple instances of cTAKES processing. To do this, run `setup_cTAKES.sh` to create cTAKES instances, and update `run_cTAKES.radiology.sh` to have the correct number of cTAKES instances that match your system needs. Do not start more than `n/2` instances of cTAKES, where `n` is the number of CPU cores available. Finally, `run_cTAKES.sh` to start cTAKES processing, and rerun `run_cTAKES.sh` as needed to process all size split files.

Within `run_cTAKES.sh`, the runPiperFile.mod.sh is a modified script that points to the correct Java, a modified piper file, and a custom dictionary. These files are available upon request.

It is **strongly** recommended to use the [gzipFiles.sh](/code/gzipFiles.sh) script (or write a custom one) to compress output XMI files, both because the subsequent step assumes the input will be gzip-compressed, and because XMI files can take up exponentially more storage space than their input file counterparts.

# Postprocessing
When completed, run `setup_for_flatfile_generation.py` to create a directory structure for flatfile processing and to create the required input files for flatfile processing. Finally, run `generate_flatfiles.v2_2d.py` to generate flatfiles from the compressed XMI files. This script will convert the XMI output to `||`-delimited text flatfiles. The output flatfiles can be over 50 GB in size, so it may be advisable to compress these files or directories.

# Additional Notes
* It is **strongly** recommended to use the [gzipFiles.sh](/code/gzipFiles.sh) script (or write a custom one) to compress output XMI files, both because the subsequent step assumes the input will be gzip-compressed, and because XMI files can take up exponentially more storage space than their input file counterparts.
* Following the preprocessing steps listed above, the medical notes were ready for the processing and analysis steps in the cTAKES overview. There is an exponential increase in processing time versus file size, so it is very important to split input files by size.
* The [preprocessing](code/preprocessing) directory contains code that was used to perform additional preprocessing on notes, and is provided as a code template for additional work that may be necessary.
* You may need to modify the `dictFromXMLTags()` function, and the `customDictKeys` hash in the main function of the `generate_flatfiles.v2_2d.py` python script.
