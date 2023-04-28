TSTRING=$(date +"%d%m%y_%H%M%S")

echo "$PWD" &>> ctakes_${TSTRING}_log.txt
./bin/runPiperFile.sh -p resources/org/apache/ctakes/clinical/pipeline/DefaultFastPipeline.piper -l resources/org/apache/ctakes/dictionary/lookup/fast/snomedonly.xml -i inputDir --xmiOut outputDir &>> ctakes_${TSTRING}_log.txt
