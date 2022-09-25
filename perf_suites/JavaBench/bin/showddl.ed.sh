USAGE="usage: showddl.ed.sh [ -x testcode ]"

TESTCODE=$(date +%y%m%d.%H%M)
while getopts :x: arguments
do
  case $arguments in
    x)  TESTCODE=${OPTARG};;   # Overrides test code
    \?) echo Invalid input switch
        echo $USAGE
        exit 1;;
  esac
done

# Show stats ED 
SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/dbconnect.properties | awk '{ print $3 }')

{
echo "set statistics on;"
echo "log temp/${TESTCODE}/${TESTCODE}.showddl.ed.log clear;"
for SCALE in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 ; do
echo "showddl $SCHEMA.ED_TABLE_$SCALE;"
done
echo "exit;"
} | ${CI_COMMAND} > /dev/null

