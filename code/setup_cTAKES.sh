CTAKESTEMPLATE=$1
INSTANCECT=$2

if [[ -z $CTAKESTEMPLATE ]] ; then
  echo 'Please provide a cTAKES executable directory for setup.'
  exit1
fi

if [[ -z $CTAKESTEMPLATE ]] ; then
  echo 'No limit for simultaneous cTAKES instances provided.'
  echo 'Assuming 10'
  let INSTANCECT=10
fi

CTAKESDIR=''
for i in $(seq 1 $INSTANCECT) ; do
  CTAKESDIR=$(echo "${CTAKESTEMPLATE}.${i}")
  cp -r $CTAKESTEMPLATE ${CTAKESDIR}
  # cTAKES directory *MUST* have subdirectories 
  # named 'inputDir' and 'outputDir'
  if [ ! -d ${CTAKESDIR}/inputDir ] ; then
    mkdir ${CTAKESDIR}/inputDir
  fi
  if [ !-d ${CTAKESDIR}/outputDir ] ; then
    mkdir ${CTAKESDIR}/outputDir
  fi
done
echo 'setup complete, ready to run cTAKES'
