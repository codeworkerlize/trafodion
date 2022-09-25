#!/bin/bash

# This script tries to check if the TRX JAR is loaded into the HBase CLASSPATH."
lv_table_name="__trafodion_check_trx_jar_$(hostname -s)"

lv_out_file=check.log

#echo "Trying to create an HBase table:'$lv_table_name' and set TrxRegionObserver as its coprocessor."
echo "Check HBase TRX configuration"
hbase shell <<EOF > ${lv_out_file} 2>&1

disable '$lv_table_name'
drop '$lv_table_name'

create '$lv_table_name', "#1"

disable '$lv_table_name'
alter '$lv_table_name' , METHOD => 'table_att' ,  'coprocessor' => '|org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionObserver|1073741823|'

enable '$lv_table_name'
EOF

#echo "Trying to describe and then put a row in the table."
#echo "If the TRX JAR is not loaded then either the describe will not show the coprocessor or the HBase put will return an error."
hbase shell <<EOF >> ${lv_out_file} 2>&1

describe '$lv_table_name'

put '$lv_table_name','r1','#1:c1','val1'

scan '$lv_table_name'

disable '$lv_table_name'
drop '$lv_table_name'
EOF

# check if the coprocessor has been setup (grep the "describe 'table'" output)
grep -q "TABLE_ATTRIBUTES.*coprocessor[$]1 => .*org.apache.hadoop.hbase.coprocessor.transactional.TrxRegionObserver" ${lv_out_file}
lv_ret=$?
if [[  ${lv_ret} == 0 ]]; then
    #echo "The coprocessor attribute seems to have been picked up. Checking whether the put succeeded."
    grep "r1 column=#1:c1" ${lv_out_file}
    lv_ret=$?
    if [[  ${lv_ret} == 0 ]]; then
      #echo "Successfully put a row in the table with the TRX coprocessor. Exitting with error code 0."
      echo "HBase TRX coprocessor successful."
    else
      echo "Error:  HBase TRX coprocessor not operational."
      echo "        Re-start HBase service before starting EsgynDB."
    fi
else
    #echo "The coprocessor attribute is not set for the table. Exitting with code 1."
    echo "Error:  HBase TRX coprocessor not operational."
    echo "        Re-start HBase service before starting EsgynDB."
fi

#echo "Output in the file: ${lv_out_file}"
exit ${lv_ret}
