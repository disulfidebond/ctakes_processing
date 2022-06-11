FLIST=$1
if [[ -z $FLIST ]] ; then
  echo 'please provide a directory name'
  exit 1
fi
ARRFILES=($(ls ${FLIST} | grep 'xmi$'))
cd $FLIST
for val in "${ARRFILES[@]}" ; do
  gzip $val
done
