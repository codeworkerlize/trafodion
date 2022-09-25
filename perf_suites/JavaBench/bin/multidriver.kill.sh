ps -ef | grep WorkloadDriver | grep -v grep | awk '{ printf "kill -s 9 %d\n",$2 }' | sh
edb_pdsh -a ps -ef | grep WorkloadDriver | grep -v grep | awk '{ print $1 $3 }'  | awk 'BEGIN { FS = ":"; } { printf "pdsh -w %s kill -s 9 %d &\n",$1,$2 }' | sh

