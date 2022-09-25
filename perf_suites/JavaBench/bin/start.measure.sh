
if [ -z "$1" ] ; then
  echo "ERROR : Excepted measure id not provided to start.measure.sh"
else

echo "
===== Start Measure ( Measure ID = $1 ) ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

startMEAS.sh $1

echo ""

fi
