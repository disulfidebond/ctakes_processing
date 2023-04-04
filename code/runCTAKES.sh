RUNDIR=$1

if [[ -z $RUNDIR ]] ; then
  echo 'ERROR, no input dir provided'
  exit 1
fi
TSTRING=$(date +"%d%m%y_%H%M%S")

ctakes_4.0.0.1_runInstance_${RUNDIR}/bin/runPiperFile.mod.sh -p resources/org/apache/ctakes/clinical/pipeline/DefaultFastPipeline.updatedComboDict.piper -l resources/org/apache/ctakes/dictionary/lookup/fast/updatedcombodict.xml -i inputDir --xmiOut outputDir &>> ctakes_${TSTRING}_log.txt
