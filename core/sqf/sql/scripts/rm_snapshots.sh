#!/bin/bash
#Get all snapshotable directories and only save the directories under /${LOBS_DIR}/trafodion/lobs/

declare -a snapshotable_dir_arr
declare -a snapshots_arr
LOBS_DIR="lobs"
#The following will just be one long string that needs to be parsed for the individual snapshotable directories
snapshotable_dir=`${MY_SW_SCRIPTS_DIR}/swhdfs lsSnapshottableDir`
#echo ${snapshotable_dir}

#Parse snapshotable_dir into an array of directories
snapshotable_dir_arr=(`echo ${snapshotable_dir} | awk -v path=${LOBS_DIR} '{for (i=1;i<NF+1;i++) if ((index($i, path) != 0)) printf "%s ",$(i);}'`)
#echo ${snapshotable_dir_arr[@]}

#If the array is empty, there is nothing to do, so just exit.
if (( "${#snapshotable_dir_arr[@]}" == 0 )); then
#	echo "no snapshotable directories"
	exit
fi

#Now, get the snapshots themselves and remove them:
for i in "${snapshotable_dir_arr[@]}"
    do
#		echo ${i}
		snapshots_arr=()
		snapshots_arr+=(`${MY_SW_SCRIPTS_DIR}/swhdfs dfs -ls ${i}/.snapshot | grep -w ${LOBS_DIR} | awk -v path=${LOBS_DIR} '{for (j=1;j<NF+1;j++) if ((index($j, path) != 0)) printf "%s ",$(j);}'`)	
#		If the array is empty, go to the next element of the directory array 
		if (( "${#snapshots_arr[@]}" == 0 )); then
#			echo ${i} "has no snapshots"
			continue
		fi
		for k in "${snapshots_arr[@]}"
			do
				snapshot_file=`echo ${k} | awk 'BEGIN {FS="/"}; {print $(NF)}'`
#				echo ${i} ${snapshot_file}
				ret=`${MY_SW_SCRIPTS_DIR}/swhdfs dfs -deleteSnapshot ${i} ${snapshot_file}`
			done
    done

#Disable the snapshotable directories	
for i in "${snapshotable_dir_arr[@]}"
	do
		ret=`${MY_SW_SCRIPTS_DIR}/swhdfs dfsadmin -disallowSnapshot ${i}`
	done
