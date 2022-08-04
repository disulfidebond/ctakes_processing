DIRLIST=$1
OUTDIR=$2

TSTRING=$(date +"%m%d%y_%H%M%S")
# START BLOCK
# input list of directories with files

if [[ -z $DIRLIST ]] ; then
  echo 'Please supply a directory list';
  exit 1
fi

if [[ -z $OUTDIR ]] ; then
  echo 'please supply an output directory name';
  exit 1
else
  if [ ! -d ${OUTDIR} ] ; then
    echo "unable to locate directory with supplied name ${OUTDIR}"
    OUTDIR=$(echo "output_dir_${OUTDIR}_${TSTRING}")
    echo 'pausing 10 seconds, then creating a new directory'
    echo "named ${PWD}/${OUTDIR}"
    sleep 10
    mkdir ${OUTDIR}
  fi
fi

ARR=($(<${DIRLIST}))
declare -a ARRFILES
for i in "${ARR[@]}" ; do 
  ARRV=() 
  ARRV+=($(ls ${PWD}/${i})) 
  for f in "${ARRV[@]}" ; do
    # debug checkpoint 0
    echo 'debug checkpoint 0'
    echo "${i}/${f}"
    # end debug checkpoint 0 
    ARRFILES+=(${i}/${f}) 
  done 
done
echo 'setup complete'
let CT=0
ROOTDIR=${PWD}
for f in "${ARRFILES[@]}" ; do
  COPYNAME=$(echo "$f" | rev | cut -d/ -f1 | rev)
  # copy files to cTAKES directory
  echo "copying input files to ctakes instance directory $CT"
  cp -r ${f} lg_ctakes_4.0.0.1_instance_${CT}/inputDir/${COPYNAME}
  ARRV=()
  ARRV=($(ls ${f}))
  # create file with list of names
  # and output file with list of processed file names
  VFILEIN=$(echo "${COPYNAME}_${TSTRING}")
  ARRVFILES+=(${VFILEIN})
  echo "writing manifest file for $VFILEIN"
  for i in "${ARRV[@]}" ; do
    k=$(echo "$i" | rev | cut -d/ -f1 | rev)
    echo "$k" >> ${VFILEIN}.inFiles.txt
  done
  echo "starting ctakes instance $CT"
  cd lg_ctakes_4.0.0.1_instance_${CT}
  echo '###' >> ctakes_log.${TSTRING}.txt
  echo '###' >> ctakes_errlog.${TSTRING}.txt
  echo "$COPYNAME" >> ctakes_log.${TSTRING}.txt
  echo "$COPYNAME" >> ctakes_errlog.${TSTRING}.txt
  ./bin/runPiperFile.mod.sh -p resources/org/apache/ctakes/clinical/pipeline/DefaultFastPipeline.modOut2.piper -l resources/org/apache/ctakes/dictionary/lookup/fast/custom_snomed.xml -i inputDir/${COPYNAME}/ --xmiOut outputDir 1>> ctakes_log.${TSTRING}.txt 2>> ctakes_errlog.${TSTRING}.txt &
  cd ${ROOTDIR}
  let CT=$CT+1
  # limiting step: only run up to 10 ctakes simultaneously
  if ((CT>15)) ; then # CHANGEME
    echo 'debug checkpoint 2, waiting for threads to complete'
    echo 'reached server limit, waiting for cTAKES instances to finish'
    wait %1 %2 %3 %4 %5 %6 %7 %8 %9 %10 %11 %12 %13 %14 %15 %16
    # wait %1 %2 %3 %4 %5
    let CTX=0
    echo "creating output manifest file $VFILEOUT"
    for x in "${ARRVFILES[@]}" ; do
      ARRVOUT=($(<${x}.inFiles.txt))
      for val in "${ARRVOUT[@]}" ; do
        if [ -f lg_ctakes_4.0.0.1_instance_${CTX}/outputDir/${val}.xmi ] ; then
          echo "${val},1" >> ${VFILEIN}.outFilesAll.txt
        else
          echo "${val},0" >> ${VFILEIN}.outFilesAll.txt
        fi
      done
    let CTX=$CTX+1
    done
    let CT=0
    # copy output and clear I/O
    ARRVFILES=()
    echo "copying output files to ${OUTDIR}"
    echo 'and clearing I/O directories in ctakes instances'
    for x in {0..15} ; do # CHANGEME
      GROUPNAME=$(ls lg_ctakes_4.0.0.1_instance_${x}/inputDir)
      mkdir ${OUTDIR}/${GROUPNAME}
      cp -r lg_ctakes_4.0.0.1_instance_${x}/outputDir ${OUTDIR}/${GROUPNAME}/
      # tar -czf ${GROUPNAME}.tar.gz ctakes_4.0.0.1_instance_${x}/outputDir
      sleep 1
      rm -rf lg_ctakes_4.0.0.1_instance_${x}/outputDir
      sleep 1
      mkdir lg_ctakes_4.0.0.1_instance_${x}/outputDir
      rm -rf lg_ctakes_4.0.0.1_instance_${x}/inputDir
      sleep 1
      mkdir lg_ctakes_4.0.0.1_instance_${x}/inputDir
    done
    echo 'finished loop of 10 directories'
  fi
done

echo 'jobs done'

