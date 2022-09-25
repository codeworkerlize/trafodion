
if [ -z "$1" ] ; then
  echo "ERROR : Excepted measure id not provided stop.measure.sh"
else

echo "
===== Stop Measure ( Measure ID = $1 ) ===== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

stopMEAS.sh $1

echo ""

fi

