function gettx {

    env | grep USE_TRANSACTIONS

}

function settx {
    export USE_TRANSACTIONS=1
    export USE_TRANSACTIONS_SCANNER=1
}

function unsettx {

    unset USE_TRANSACTIONS
    unset USE_TRANSACTIONS_SCANNER

}

function jmaphb {

# get the HMaster pid
    lv_hbpid=`jps | grep HMaster | cut -d' ' -f1`
    if [ ! -z ${lv_hbpid} ]; then
	echo "HMaster pid: ${lv_hbpid}"
    else
	echo "There's no HMaster process"
	return
    fi

# execute jmap on the pid
    $JAVA_HOME/bin/jmap -histo:live ${lv_hbpid} 
}

export -f settx
export -f unsettx
export -f gettx
export -f jmaphb

