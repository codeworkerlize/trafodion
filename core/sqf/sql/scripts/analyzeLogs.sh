#!/bin/sh

# 输出结果目录，最终的结果只需要查看文件：trans_jitter_result.txt
outputDir="/tmp/result"
# 优先使用指定的数据库日志路径，如果不指定就取当前节点的默认数据库路径${TRAF_LOG}
trafLogDir=""
# 优先使用指定的数据库日志路径，如果不指定就取当前节点HBase命令行参数的日志路径${hbase.log.dir}
hbaseLogDir=""
# 如果LOG时间太长，想通过时间过滤掉没有用的LOG，可以指定startTime和endTime来进行过滤
# 时间可以指定如有格式的日期：2020-12-05 21:30:10，也可以是前缀部分
startTime="2020-12-07"
endTime="2020-12-07"
# 如果想删除掉中间结果，重新拷贝日志可以指定-r参数
needRemove="false"
# 指定过滤的抖动比例，默认10倍，一般抖动没有这么大，如果发现没有结果，可以适当减小该值
jitterQuota=10
# 指定过滤事务的执行时间，默认是1000ms，意思是指定超过1秒的才会分析
jitterQuotaTime=3000

# 局部变量
gcfiles=0
MYSELF=`trafconf -myName`

## 过滤关键字
trafKey="THIS_TRAN"
selectKey="ExHbaseSelect summary"
iudKey="ExHbaseIUD summary"
scannerKey="TransactionalScanner summary"
stateKey="TransactionalTable summary"
corproKey=" copro PID "

echo "a.  显示所有的用户参数："
while getopts ":o:s:e:rvh" opt
do  
    case "$opt" in
        o)
            outputDir=$OPTARG
            echo "    结果文件输出路径：[${outputDir}]"
            shift 2 ;;

        s)
            startTime="$OPTARG*"
            echo "    LOG的开始时间：[${startTime}]"
            shift 2 ;;

        e)
            endTime="$OPTARG*"
            echo "    LOG的结束时间：[${endTime}]"
            shift 2 ;;

        r)  
            echo "    指定强制删除历史数据"
			needRemove="true"
            ;;

        h)  
            usage 0 ;;

        *)  
            usage 1 ;;
    esac
done

# the functions                                                                                                                
usage(){
    echo -e "命令: $0 [OPTIONS]"
    echo -e ""
    echo -e "语法："
    echo -e "    $0] [-o outputDir] [-s startTime] [-e endTime] [ -r | -v | -h]"
    echo -e ""
    echo -e "选项(OPTIONS)："
    echo -e "    -o <outputDir>     结果文件输出路径"
    echo -e "    -s <startTime>     需要分析的LOG的开始时间"
    echo -e "    -e <endTime>       需要分析的LOG的结束时间"
    echo -e "    -r                 删除现有的文件重新从数据库拷贝"
    echo -e "    -v                 打印[$0]版本信息并退出"
    echo -e "    -h                 打印[$0]帮助信息并退出"
    echo -e ""

    exit $1
}

version(){
    echo -e "测试结果分析脚本版本号[0.9.0.0]"
    exit 0
}

function backup_log_to_output(){
    sourceLogsDir=$1
    subDir=${outputDir}/$2_logs
    prefix=$3
    filter=$4
    mkdir -p ${subDir}

    if [ ! -d ${sourceLogsDir} ]; then
		# 显示黄色警告信息
		echo -e "\033[33m    - 警告：指定日志路径不存在，目前指定的是[${sourceLogsDir}] \033[0m"
		return 1
    fi
    echo "    - 备份指定时间段的${2}日志到目录[${subDir}]"
	
    tmpDir=${outputDir}/tmp

    echo "#!/bin/sh

mkdir -p ${tmpDir}

lognum=0
if [ \"x${filter}\" = \"x\" ]; then
    filePaths=\`find ${sourceLogsDir} -name \"${prefix}*.log*\" \`
else
    filePaths=\`find ${sourceLogsDir} -name \"${prefix}*.log*\" | grep -v ${filter}\`
fi

for filePath in \${filePaths}
do
	let lognum++;
	fileName=\$(basename \${filePath});
	echo \"      \${lognum}) 处理[\${filePath}]\"
	echo \"         生成文件[${subDir}/\${fileName}.\${lognum}]\"
	cat \${filePath} | sed -n \"/${startTime}*/,/${endTime}*/p\" >> ${tmpDir}/\${fileName}.\${lognum}
done
files=\`ls ${tmpDir}/${prefix}*.log* 2>&1 | grep -v \"No such file\" | wc -l\`
if [ \${files} -gt 0 ]; then
    scp ${tmpDir}/${prefix}*.log* ${MYSELF}:${subDir}
else
    echo -e \"\033[33m    - 警告：指定路径不存在文件[${sourceLogsDir}/\"${prefix}*.log*\"] \033[0m\"
fi
rm -rf ${tmpDir}" > /tmp/filterLogs.sh
    chmod +x /tmp/filterLogs.sh
    edb_pdsh -a "scp ${MYSELF}:/tmp/filterLogs.sh /tmp; /tmp/filterLogs.sh; rm -rf /tmp/filterLogs.sh"

    return 0
}

function filter_get_value(){
    logFile=$1
    filterKey=$2
    columnNum=$3

    value=`cat ${logFile} | grep "${filterKey}" | head -n 1 | awk -v cnum=${columnNum} '{ print $cnum }'`

    if [ "x${value}" = "x" ]; then
		value=0
    fi
    
    echo ${value}
    # return ${value}
}

function reset_env(){
	if [ "x${needRemove}" = "xtrue" ]; then
		rm -rf ${outputDir}
	else
		rm -rf ${transLogDir}
	fi

	mkdir -p ${transLogDir}

	if [ ! -d ${targetTrafDir} ]; then
		backup_log_to_output ${trafLogDir} traf trafodion "\"\""
		backup_log_to_output ${trafLogDir} traf tm "\"\""
	else
		echo -e "\033[33m    - 警告：Trafodion日志目录[${targetTrafDir}]已经存在,如果想重新拷贝请删除此目录或者指定-r参数!\033[0m"
	fi

	if [ ! -d ${targetHbaseDir} ]; then
		backup_log_to_output ${hbaseLogDir} hbase hbase "\"\""
		if [ -d ${hbaseLogDir} ]; then
			gcfiles=`ls ${hbaseLogDir}/*gc* 2>&1 | grep -v "No such file" | wc -l`
			if [ ${gcfiles} -gt 0 ]; then
				edb_pdsh -a "cat ${hbaseLogDir}/*gc* > /tmp/gc.\`hostname\`; scp /tmp/gc.* ${MYSELF}:${targetHbaseDir}"
			else
				echo -e "\033[33m    - 警告：HBase日志目录[${hbaseLogDir}]中没有gc日志文件!\033[0m"
			fi
		fi
		edb_pdsh -a "scp ${hbaseLogDir}/trafodion.lockwait.log ${MYSELF}:${targetHbaseDir}/\`hostname\`.trafodion.lockwait.log"
	else
		echo -e "\033[33m    - 警告：HBase日志目录[${targetHbaseDir}]已经存在,如果想重新拷贝请删除此目录或者指定-r参数!\033[0m"
	fi
}

echo ""
echo "b.  备份指定时间段的日志到目录[${outputDir}]"
transLogDir=${outputDir}/transactions
if [ "x${trafLogDir}" =  "x" ]; then
	trafLogDir="${TRAF_LOG}"
fi

if [ "x${hbaseLogDir}" = "x" ]; then
	hbaseLogDir=`ps -www -ef | grep -v grep | grep proc_regionserver | awk -F"-Dhbase.log.dir=" '{ print $2 }' | awk '{ print $1 }'`
	if [ "x${hbaseLogDir}" = "x" ]; then
		hbaseLogDir=${HBASE_HOME}/logs
	fi
fi

targetHbaseDir=${outputDir}/hbase_logs
targetTrafDir=${outputDir}/traf_logs
reset_env

echo ""
echo "c.  处理备份的HBase的GC日志文件"
gcfiles=`ls ${targetHbaseDir}/*gc* 2>&1 | grep -v "No such file" | wc -l`
if [ ${gcfiles} -gt 0 ]; then
    cat ${targetHbaseDir}/*gc* | grep "Total time for which application threads were stopped" > ${transLogDir}/gc.stopped.log
else
    echo "" > ${transLogDir}/gc.stopped.log
    echo -e "\033[33m    - 警告：没有需要处理的GC日志，请确认数据库日志文件路径指定正确 \033[0m"
fi

echo ""
echo "d.  处理备份的数据库日志文件"
if [ -d ${trafLogDir} ]; then
	trafLogs=`ls -A ${trafLogDir}`
fi

if [ "x${trafLogs}" = "x" ]; then
    echo -e "\033[33m    - 警告：没有需要处理的数据库日志，请确认数据库日志文件路径指定正确 \033[0m"
else
    echo "    - 处理数据库日志"
    echo "      1) 生成数据库的事务信息，并存入文件[${transLogDir}/traf_trans.log]"
    find ${targetTrafDir} -name trafodion.mxosrvr.*.log* | xargs cat | grep ${trafKey} >> ${transLogDir}/traf_trans.log

    echo "      2) 过滤掉事务信息文件的无用信息，排序并存入文件[${transLogDir}/traf_trans_ordered.log]"
	# 示例：2020-12-02 06:33:31.814, WARN, MXOSRVR, Node Number: 0, CPU: 0, PID:11305, Process Name:$Z0000000C19 , , ,DISPATCH_TCPIPRequest THIS_TRAN PID 11305 txID 72339069015678612 SCC 54 SATC 31 STTC 1695 TC 1 TATC 1695 TTC 1695 NO_INFO
    # 事务时间          进程类型 节点号 进程号 进程名 接口  子类型  事务号 语句数 语句平均时间 事务耗时 事务总数 平均耗时 事务总耗时 事务类型 抖动比例
    # TTIME             PTYPE    NID    PID    PNAME  API   SubType txID   SCC    SATC         STTC     TC       TATC     TTC        CliInfo  STTC/TATC        
    # $1"T"$2":"$3":"$4 $6       $9     $13    $16    $17   $18     $22    $24    $26          $28      $30      $32      $34        $35      $28/$32
    cat ${transLogDir}/traf_trans.log | grep -v "txID \-1" | awk -F '[\\[\\]<>,: ]+' '{ print $1"T"$2":"$3":"$4" "$6" "$9" "$13" "$16" "$17" "$18" "$22" "$24" "$26" "$28" "$30" "$32" "$34" "$35" "$28/$32 }' | sort -n -r -k 16 >> ${transLogDir}/traf_trans_ordered.log
    cat ${transLogDir}/traf_trans.log | grep "txID \-1" | awk -F '[\\[\\]<>,: ]+' '{ print $1"T"$2":"$3":"$4" "$6" "$9" "$13" "$16" "$17" "$18" "$22" "$24" "$26" "$28" "$30" "$32" "$34" "$35" "$28/$32 }' | sort -n -r -k 11 >> ${transLogDir}/traf_trans_ordered.log

    echo "      3) 过滤事务信息文件，并把抖动的事务存入文件[${transLogDir}/traf_trans_jitter.log]"
    echo "事务时间 进程类型 节点号 进程号 进程名 接口 子类型 事务号 语句数 语句平均时间 事务耗时 事务总数 平均耗时 事务总耗时 事务类型 抖动比例" > ${transLogDir}/traf_trans_jitter.log
    # 抖动比例大于给的值${jitterQuota}，就需要进行分析
    # 虽然没有抖动，但执行时间大于${jitterQuotaTime}也进行分析
    cat ${transLogDir}/traf_trans_ordered.log | awk -v quota=${jitterQuota} -v quotaTime=${jitterQuotaTime} '$16>quota || $11>quotaTime { print $0 }' >> ${transLogDir}/traf_trans_jitter.log
    echo ""
fi

jitterFile=""
if [ -f ${transLogDir}/traf_trans_jitter.log ]; then
    jitterFile=${transLogDir}/traf_trans_jitter.log
else
    echo -e "\033[32m日志处理成功，没有发现有抖动！\033[0m"
    exit 0
fi

jitterNum=`cat ${jitterFile} | wc -l`
echo "    - 读取抖动的事务，并逐个处理$(($jitterNum-1))数据库日志"
tidnum=0
echo "事务时间 事务号 事务类型 事务总数 事务耗时 平均耗时 抖动比例 select耗时 iud耗时 scanner耗时 table耗时 RPC耗时 RPC最大耗时 corpro耗时 corpro最大耗时 发送耗时 发送最大耗时 RPC调用次数 HBase耗时 HBase最大耗时 HBase调用次数 GC时间 GC耗时" >> ${transLogDir}/trans_jitter_result.txt
# 事务时间          进程类型 节点号 进程号 进程名 接口  子类型  事务号 语句数 语句平均时间 事务耗时 事务总数 平均耗时 事务总耗时 事务类型 抖动比例
# TTIME             PTYPE    NID    PID    PNAME  API   SubType txID   SCC    SATC         STTC     TC       TATC     TTC        CliInfo  STTC/TATC        
cat ${jitterFile} | awk 'NR>1' | while read ttime ptype nid pid pname api subtype txID scc satc sttc tc tatc ttc cliinfo jitter
do
    if [ "x${txID}" != "x-1" ]; then
	let tidnum++
	echo "      ${tidnum}) 处理事务${txID}, 生成文件[${transLogDir}/${txID}.log]"
	find ${targetTrafDir} -name trafodion.*.log* | xargs cat | grep ${txID} >> ${transLogDir}/${txID}.log
	find ${targetTrafDir} -name tm*.log* | xargs cat | grep ${txID} >> ${transLogDir}/tm_${txID}.log
	# 2020-12-02 10:55:01,881, WARN, SQL.EXE, Node Number: 0, CPU: 0, PIN: 22132, Process Name: $Z0000000NLM,,,ExHbaseSelect summary PID 22132 txID 72339069015718798 TCC 66 TTC 0 FILE: ../executor/ExHbaseSelect.cpp LINE: 1652
	selectTime=$(filter_get_value ${transLogDir}/${txID}.log "${selectKey}" 23)
	# 2020-12-02 10:55:01,882, WARN, SQL.EXE, Node Number: 0, CPU: 0, PIN: 22132, Process Name: $Z0000000NLM,,,ExHbaseIUD summary PID 22132 txID 72339069015718798 TCC 93 TTC 2 FILE: ../executor/ExHbaseIUD.cpp LINE: 2266
	iudTime=$(filter_get_value ${transLogDir}/${txID}.log "${iudKey}" 23)

	cat ${targetTrafDir}/trafodion.sql.java.*.log* | grep ${txID} >> ${transLogDir}/java_${txID}.log
	# 2020-12-02 10:54:50.000 22132@centos-esgyn_qian_1.6 [main] ,WARN ,org.apache.hadoop.hbase.client.transactional.TransactionalScanner ,TransactionalScanner summary PID 22132 txID 72339069015718601 TCC 1 TTC 1
	scannerTime=$(filter_get_value ${transLogDir}/java_${txID}.log "${scannerKey}" 16)
	# 2020-12-02 10:55:11.054 22132@centos-esgyn_qian_1.6 [main] ,WARN ,org.apache.hadoop.hbase.client.transactional.TransactionalTable ,TransactionalTable summary PID 22132 txID 72339069015718964 TCC 33 TTC 8 TLTC 0
	tableTime=$(filter_get_value ${transLogDir}/java_${txID}.log "${stateKey}" 16)

	# 2020-12-02 10:55:01.822 22132@centos-esgyn_qian_1.6 [main] ,WARN ,org.apache.hadoop.hbase.client.transactional.TransactionalTable ,put copro PID 22132 txID 72339069015718798 CC 1 ATC 0 TTC 0 TC 0 0 0 0 TRAF_1500000:TRAFODION.BMS_8W.BMSQL_DISTRICT
	rpcTime=`cat ${transLogDir}/java_${txID}.log | grep "${corproKey}" | awk '
BEGIN{
    sum21=0;
    max21=0;
    sum22=0;
    max22=0;
    sum23=0;
    max23=0;
}
{
    sum21+=$21;
    if($21+0>max21+0){
        max21=$21;
    };

    sum22+=$22;
    if($22+0>max22+0){
        max22=$22;
    };

    sum23+=$23;
    if($23+0>max23+0){
        max23=$23;
    };

    # print "["$0"]sum21:"sum21" max21:"max21" sum22:"sum22" max22:"max22" sum23:"sum23" max23:"max23" NR:"NR
} 
END { 
    print sum21" "max21" "sum22" "max22" "sum23" "max23" "NR 
}'`

	cat ${hbaseLogDir}/hbase-*.log* | grep "${txID}" | grep txID | grep WARN | grep inner > ${targetHbaseDir}/hbase_${txID}.log
	hbaseTime=`cat ${targetHbaseDir}/hbase_${txID}.log | awk '
BEGIN{
    sum=0;
    max=0;
}
{
    sum+=$13;
    if($13+0>max+0){
        max=$13;
    }
} 
END { 
    print sum" "max" "NR 
}'`

	
	tranEndTimestamp=$(date +%s -d ${ttime})
	interval=`echo ${sttc} | awk -F. '{ print $1 }'`
	tranStartTimestamp=$[${tranEndTimestamp}-${interval}/1000]
	#  2020-12-02T10:52:32.070+0000: 0.292: Total time for which application threads were stopped: 0.0000982 seconds, Stopping threads took: 0.0000146 seconds
	gcTime=`cat ${transLogDir}/gc.stopped.log | awk -v tst=${tranStartTimestamp} -v tet=${tranEndTimestamp} '
BEGIN {
    sum=0;
    start=0;
} 
{
    time=$1
    gsub("-", " ", time)
    gsub("T", " ", time)
    gsub(":", " ", time)
    gsub("+", " ", time)
    st=mktime(time);
    if(st+0>=tst+0 && st+0<=tet+0){
        sum+=$11;
        if(start == 0){
            start=st;
        }
        # print "["$0"] sum:"sum" gc:"$11" tst:"strftime("%Y-%m-%dT%H:%M:%S", tst)" tet:"strftime("%Y-%m-%dT%H:%M:%S", tet)" NR:"NR
    }
} 
END{
    if(start != 0) {
        print strftime("%Y-%m-%dT%H:%M:%S", start)" "sum*1000
    } else {
        print "- "sum*1000
    }
}'`

	echo "${ttime} ${txID} ${cliinfo} ${tc} ${sttc} ${tatc} ${jitter} ${selectTime} ${iudTime} ${scannerTime} ${tableTime} ${rpcTime} ${hbaseTime} ${gcTime}" >> ${transLogDir}/trans_jitter_result.txt
    elif [ ! -f ${transLogDir}/others.log ]; then
	let tidnum++
	echo "      ${tidnum}) 处理事务${txID}, 生成文件[${transLogDir}/others.log]"
	find ${targetTrafDir} -name trafodion.mxosrvr.*.log* | xargs cat | grep "tid: \-1" >> ${transLogDir}/others.log
    fi
done

echo -e "\033[32m分析日志成功！请查看文件[${transLogDir}/trans_jitter_result.txt]查看分析最终结果。\033[0m"

