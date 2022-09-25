USAGE="usage: test.summary.sh [ -id|--testid|testid <<testid>> ]
  Dumps the summary information of a performance curve test in csv compatible format
  Options:
    [ -h|--help|help ]
"

TESTID=default

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -id|--testid|testid)			export TESTID="$1"; shift;;
    -h|--help|help)				echo -e "$USAGE"; exit 1;;
    *)						echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [[ ${TESTID} = "default" ]] ; then
echo "Required TESTID not provided."; echo -e "$USAGE"; exit 1;
fi

echo "TestID,${TESTID}"
echo "Benchmark,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep -m 1 'BENCHMARK' | awk '{ print $3 }')"
echo "Workload,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep -m 1 'WORKLOAD' | awk '{ print $3 }')"
echo ""
echo "TestID,${TESTID}"
echo "Concurrency,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep Concurrency | awk '{ print $3 }')"
echo "Throughput,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep Throughput | awk '{ print $4 }')"
echo "AvgLatency,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep 'Average Latency (' | awk '{ print $5 }')"
echo "PathLength,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep '\[CPU\]Totl' | awk '{ print $3 }')"
echo "NetTxKB,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep '\[NET\]TxKBTot' | awk '{ print $3 }')"
echo "DiskReadKB,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep '\[DSK\]ReadKBTot' | awk '{ print $3 }')"
echo "DiskWriteKB,$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep '\[DSK\]WriteKBTot' | awk '{ print $3 }')"
echo ""
echo "$(cat ${JAVABENCH_LOG_HOME}/${TESTID}.*/${TESTID}.run.*.log | grep '^XXX' | awk '{ print $3 "," $15 }')"
echo ""


