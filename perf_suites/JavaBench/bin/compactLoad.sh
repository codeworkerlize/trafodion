#! /bin/bash
#  Gag   141111   Initial version.
#  Gag   150129   Add the flush option.
#  Gag   150220   Adding TPCH.  Fixed ED and YCSB to work when scale is specified.
#        150223   Added np which gives count per salt value.
#        150305   Adding hbase ycsb.
#        150415   Adding enable disable options.  Support TLOGs.
#        150422   Option to overwrite CATALOG.SCHEMA.
#        150423   Option to specify table.
#        150429   Add GRANT/REVOKE option.

NAMEMGR_URL="${SYSTEM_NAMEMGR_URL}"
NAMEMGR_PORT=${SYSTEM_NAMEMGR_PORT}

usage_help ()
{
    echo "Syntax: $0 [-e|-d|-f|-c|-n|-p|-g <opt>|-r|-x] -l OE | DC | Y[h] | ED | RP | PH | TL -s <scale> -o <cat.sch> -t <table> -u <user>"
    echo "To do major_compact on the tables for the specified load."
    echo "-l to specify which workload to perform operation on."
    echo "-t to specify a specific table to perform operation on."
    echo "-f does flushes instead of compacts."
    echo "-c does regular minor compacts instead of major."
    echo "-n does select count(*)."
    echo "-e enables tables, -d disables tables."
    echo "-p does select count(*) per salt value. (will return error if not salted)."
    echo "-g grant <opt> privilege to the current user (if -u not specified) for the specified workload."
    echo "   <opt> can be a string with multiple permissions.  Use quotes if spaces."
    echo "-r revokes all privilges from the current user (if -u not specified) for the specified workload."
    echo "-x cleans up the tables."
    echo "-u <user> for grant/revoke, specifies which user to do privilege operation for."
    echo "   <opt> can be a string with multiple users.  Use quotes if spaces."
    echo "-o overwrites default catalog schema."
    exit 0
}

if [ $# -eq 0 ] ; then
    usage_help
fi

wantLoad=""
wantTable=""
wantScale=0
wantCatalog="TRAFODION."
wantSchema=JAVABENCH
wantCmd="major_compact"
wantUser=$(whoami)
wantOpt=""

while getopts 'hl:s:cfnpedo:t:g:ru:x' parmOpt
do
    case $parmOpt in
    c) wantCmd="compact";;
    d) wantCmd="disable";;
    e) wantCmd="enable";;
    f) wantCmd="flush";;
    g) wantCmd="grant"
       wantOpt="${OPTARG}";;
    h) usage_help
       exit 0;;
    l) case ${OPTARG^^} in
       OE) wantLoad=OE_;;
       DC) wantLoad=DC_;;
       Y) wantLoad=YCSB_TABLE
          wantScale=-1;;
       YH) wantLoad=YCSB_TABLE
           wantSchema=YCSB
           wantCatalog=""
           wantScale=-1;;
       ED) wantLoad=ED_TABLE
           wantScale=-1;;
       RP) wantLoad=METRIC_
           wantSchema=_REPOS_
           wantScale=-1;;
       PH) wantLoad=TPCH_;;
       TL) wantLoad=TLOG
           wantSchema=_DTM_
           wantScale=1;;
       *) echo "Invalid load specified."
          exit 0;;
       esac
       ;;
    n) wantCmd="count";;
    o) wantCatalog=""
       wantSchema=$OPTARG;;
    p) wantCmd="count/salt";;
    r) wantCmd="revoke";;
    s) wantScale=$OPTARG;;
    t) wantTable=$OPTARG
       wantScale=-1;;
    u) wantUser="$OPTARG";;
    x) wantCmd="cleanup";;
    ?)  echo "Invalid option specified.   Only -c,-d,-e,-f,-g,-l,-n,-o,-p,-l,-s,-t,-u,-x and -h are allowed."
        exit 0 ;;
    esac
done

if ([ ${#wantLoad} -eq 0 ] && [ ${#wantTable} -eq 0 ]) || [ ${wantScale} -eq 0 ] ; then
    echo "You must specify a load or table to compact/flush/count/etc and a scale to use."
    exit 0
fi

if [ ${#wantTable} -gt 0 ] ; then
	echo "Specified specific table ${wantTable}."
	case $(echo ${wantTable} | grep -cF -e '.') in
	0) wantLoad=${wantTable};;
	1) read -r wantSchema wantLoad <<< $(echo $wantTable | awk -F. '{print $1,$2}')
	   wantCatalog=""
	   ;;
	2) read -r wantCatalog wantSchema wantLoad <<< $(echo $wantTable | awk -F. '{print $1,$2,$3}');;
	*) echo "Can not parse tablename ${wantTable} into cat.sch.table."
	   exit 0
	   ;;
	esac
	wantScale=0
fi

if [ ${wantLoad} = TLOG ] ; then
    if [ ${wantScale} -gt 0 ] ; then
        wantLoad=TLOG${wantScale}
        wantScale=""
    fi
elif [ ${wantScale} -gt 0 ] ; then
    wantScale=_${wantScale}
else
    wantScale=""
fi
echo "Preparing to ${wantCmd} ${wantCatalog}${wantSchema}.${wantLoad}*${wantScale}"

for plate in ${wantLoad}
do
    listCmd="list '${wantCatalog}${wantSchema}.*${plate}.*${wantScale}'
${listCmd}"
done
             
tableList=$(echo "${listCmd}" | hbase shell 2>/dev/null | \
    awk 'BEGIN {want=0}
         /^TABLE/ {want=1; next}
         /row\(s\)/ {want=0;next}
                  {if (want==1) {print $1}}
        ')

if [ ${#tableList} -eq 0 ] ; then
    echo "No tables were found."
    exit 0
fi

if [ ${wantCmd} = "count" ] ; then
    if [ ${#wantCatalog} -eq 0 ] ; then
        #This is an hbase table.
        tableList=$(echo "${tableList}" | sed -e "s/^/hbase.\x22_ROW_\x22.\x22/g" -e 's/$/\x22/g')
    fi
    countTableList=$(echo "${tableList}" | sed -e "s/^/select count(*) from /g" -e 's/$/;/g' -e 's/_REPOS_/\x22_REPOS_\x22/g')
    echo "${countTableList}"
    echo "${countTableList}" | sqlci
    exit 0
elif [ ${wantCmd} = "count/salt" ] ; then
    countTableList=$(echo "${tableList}" | sed -e "s/^/select \x22_SALT_\x22, count(*) from /g" -e 's/$/ group by 1 order by 1;/g' -e 's/_REPOS_/\x22_REPOS_\x22/g')
    echo "${countTableList}"
    echo "${countTableList}" | sqlci
    exit 0
elif [ ${wantCmd} = "grant" ] ; then
    countTableList=$(echo "${tableList}" | sed -e "s/^/grant ${wantOpt} on /g" -e "s/\$/ to ${wantUser};/g" -e 's/_REPOS_/\x22_REPOS_\x22/g')
    echo "${countTableList}"
    echo "${countTableList}" | sqlci
    exit 0
elif [ ${wantCmd} = "revoke" ] ; then
    countTableList=$(echo "${tableList}" | sed -e "s/^/revoke all on /g" -e "s/\$/ from ${wantUser};/g" -e 's/_REPOS_/\x22_REPOS_\x22/g')
    echo "${countTableList}"
    echo "${countTableList}" | sqlci
    exit 0
elif [ ${wantCmd} = "cleanup" ] ; then
    countTableList=$(echo "${tableList}" | sed -e "s/^/cleanup table /g" -e "s/\$/;/g" -e 's/_REPOS_/\x22_REPOS_\x22/g')
    echo "${countTableList}"
    echo "${countTableList}" | sqlci
    exit 0
else
    compTableList=$(echo "${tableList}" | sed -e "s/^/${wantCmd} \x27/g" -e 's/$/\x27/g') 
    echo "${compTableList}" | hbase shell
#    echo "${compTableList}"
#    exit 0
fi

echo ""
echo "Now verifying that all tables have been ${wantCmd}ed."
echo ""

for currTable in ${tableList}
do
    while true ; do
        read -r enabledStat compStat <<< $(curl -sS --noproxy '*' http://${NAMEMGR_URL}:${NAMEMGR_PORT}/table.jsp?name=${currTable} 2>/dev/null | head -100 | \
            tr '<>' ',,' | awk -F, 'BEGIN {want=0; found=0; enaStat=""; compStat=""}
                 /Table Attributes/ {want=1;next}
                 /Compaction/ {if (want==1) {want=2; next}}
                 /Enabled/    {if (want==1) {want=3; next}}
                 /\/td/ {if (found>=2) {print enaStat, compStat; exit}}
                 {if (want==2) {if (NF==1) {compStat=$1; found++; want=1} else 
                                if (NF>3)  {compStat=$3; found++; want=1}}}
                 {if (want==3) {if (index($0,"false") > 0) {enaStat="FALSE"; found++; want=1} else
                                if (index($0,"true")  > 0) {enaStat="TRUE";  found++; want=1}}}
                ')
        echo "Table: ${currTable} - ${wantCmd}: Enabled:  ${enabledStat}, Compaction: ${compStat}"
        case ${wantCmd} in
        enable)  if [ X${enabledStat} == XTRUE ]  ; then   break ;  fi ;;
        disable) if [ X${enabledStat} == XFALSE ] ; then   break ;  fi ;;
        *)       if [ X${compStat} == XNONE ] ; then       break ;  fi ;;
        esac
        sleep 5
    done
done
     
exit 0
