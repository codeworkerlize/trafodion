
echo "
===== JavaBench Scripts  ====== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"
echo "        JAVABENCH_HOME = ${JAVABENCH_HOME}"

echo "
===== System Under Test (SUT) Version Information ====== [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

pdsh -w $SYSTEM_FIRST_NODE sqvers -u

echo "
    Trafodion CQD Settings (SQSystemDefaults.conf): 
"
pdsh -w $SYSTEM_FIRST_NODE cat ${TRAF_CONF}/SQSystemDefaults.conf

if [ ! -z "${TEST_CQD_FILE}" ] ; then
echo "
      TEST_CQD_FILE environment variable set :
"
echo "        TEST_CQD_FILE = ${TEST_CQD_FILE}"
echo "        cat TEST_CQD_FILE :
"
cat ${TEST_CQD_FILE}
fi

echo "
    Tlog/Hlog Settings: "
echo "
      trafodion settings (ms.env file) :
"
pdsh -w $SYSTEM_FIRST_NODE grep TLOG_WRITE $SQ_VAR/ms.env

echo "
      Process Counts :
"
pdsh -w $SYSTEM_FIRST_NODE sq_check_myuserid -c

echo ""
