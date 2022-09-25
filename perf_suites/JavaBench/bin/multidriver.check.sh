edb_pdsh -a ps -ef | grep WorkloadDriver | grep -v grep | awk '{ printf "%s%d\n",$1,$3}' 
edb_pdsh -a ps -ef | grep WorkloadDriver | grep -v grep | awk '{ printf "%s%d\n",$1,$3}' | wc -l
