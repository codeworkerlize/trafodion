USAGE="usage: releasetest.summary.sh [ -x testid ] "
TESTID=${TESTCODE}
while getopts :x: arguments
do
  case $arguments in
    x)  TESTID=${OPTARG};;   # Overrides test code
    \?) echo Invalid input switch
        echo $USAGE
        exit 1;;
  esac
done

echo "
Test : $TESTID"

echo "
Test1YCSBSingletonSelect"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.1.* | awk '
/^XXX/ { print $3 " : " $15 }
/^       Throughput / { 
  print "" 
  print "Throughput : " $4
  }
/^   Average Latency / { 
  print "AvgLatency/Response : " $5
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test2YCSBSingleton5050"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.2.* | awk '
/^XXX/ { print $3 " : " $15 }
/^       Throughput / { 
  print "" 
  print "Throughput : " $4
  }
/^   Average Latency / { 
  print "AvgLatency/Response : " $5
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test3YCSBSingletonUpdate"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.3.* | awk '
/^XXX/ { print $3 " : " $15 }
/^       Throughput / { 
  print "" 
  print "Throughput : " $4
  }
/^   Average Latency / { 
  print "AvgLatency/Response : " $5
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test4DebitCreditAutoCommit"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.4.* | awk '
/^XXX/ { print $3 " : " $15 }
/^       Throughput / { 
  print "" 
  print "Throughput : " $4
  }
/^   Average Latency / { 
  print "AvgLatency/Response : " $5
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test5DebitCreditTransactional"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.5.* | awk '
/^XXX/ { print $3 " : " $15 }
/^       Throughput / { 
  print "" 
  print "Throughput : " $4
  }
/^   Average Latency / { 
  print "AvgLatency/Response : " $5
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test6EDAtomics"
echo "CountTest
"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.6.* | awk '/^          count/ { print $3 " : " $7 }'
echo "
2MinScale : 2MinScale
"
echo "ScanTest
"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.6.* | awk '/^           scan/ { print $3 " : " $7 }'
echo "
2MinScale : 2MinScale
"
echo "OrderByTest
"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.6.* | awk '/^        orderby/ { print $3 " : " $7 }'
echo "
2MinScale : 2MinScale
"
echo "GroupByTest
"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.6.* | awk '/^        groupby/ { print $3 " : " $7 }'
echo "
2MinScale : 2MinScale"

echo "
Test7DataLoadYCSB"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.7.* | awk '
/^XXX/ { print $3 " : " $15 }
/^   Total ,   / { 
  print "" 
  print "Throughput : " $15
  print "AvgLatency/Response : " $17
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test8DataLoadED"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.8.* | awk '
/^XXX/ { print $3 " : " $15 }
/^   Total ,   / { 
  print "" 
  print "Throughput : " $15
  print "AvgLatency/Response : " $17
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test9DataLoadDebitCredit"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.9.* | awk '
/^XXX/ { print $3 " : " $15 }
/^   Total ,   / { 
  print "" 
  print "Throughput : " $15
  print "AvgLatency/Response : " $17
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

echo "
Test11OrderEntry"
cat ${JAVABENCH_LOG_HOME}/${TESTID}.run.releasetest/${TESTID}.11.* | awk '
/^XXX/ { print $3 " : " $15 }
/^       Throughput / { 
  print "" 
  print "Throughput : " $4
  }
/^   Average Latency / { 
  print "AvgLatency/Response : " $5
  }
/^           \[CPU\]Totl /  {
  print "PathLength : " $3
}'

