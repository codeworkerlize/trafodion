# Collectl could have up to 11 attributes.
#    Will have to verify order of parms and also if IRQ, SOFT and STEAL are used or not.
#    V 1.3  140820   Initial version taken from loadMeasCPU.awk.
#
BEGIN {IGNORECASE = 1; doneTitle = 0; numFields = 0; numDateTimeFields = 1; sumList="," ; }

/^#/ || /^\[/ {
#   print "In Date", $0
   # This is the title line.   We have to parse it to determine the version of collectl, the number of fields and number of cpus.
   if ( $2 == "Time" ) {
       numDateTimeFields = 2
   } else if ( substr($1,1,5) == "#Date" ) {
       numDateTimeFields = 1
   } else {
       numDateTimeFields = 0
   }
   if ( doneTitle == 0 ) {
       gsub("%", "")
       for ( i = numDateTimeFields + 1 ; i <= NF ; i++ ) {
           if ( wantSum == 1 ) {
              wantField = 0
              if (index($i, "[CPU]") > 0) {
                  if (index("Use,Nic,Sys,Tot", substr($i,6,3)) > 0) wantField = 1
              } else if (index($i, "[DSK]") > 0) {
                  if (index("ReadTo,WriteT,ReadKB,WriteK", substr($i,6,6)) > 0) wantField = 1 ;
              } else if (index($i, "[NET]") > 0) {
                  if (index("RxP,TxP,RxK,TxK,RxE,TxE", substr($i,6,3)) > 0) wantField = 1 ;
              }
              if (wantField == 1) {
                  if (wantSumCol == 0) {
                      printf "%14.14s ", $i
                  } else {
                      colTtle[i] = $i
                  }
                  sumList = sumList i ","
              }
           } else {
              printf "%s ", $i
           }
       }
       if (wantSumCol == 0) printf "\n"
       doneTitle = 1
   }
   next
}  

{ #  This is the normal processing.
#   print "In Normal", $0
   if ( length($1) == 0 ) next
   
   for ( i = numDateTimeFields + 1; i <= NF; i++ ) {
       col[i] = col[i] + $i
   }
   if (NF > numFields) numFields = NF
   next
}

END {
   if (wantSumCol == 0) {
       for ( i = numDateTimeFields + 1 ; i <= numFields ; i++ ) {
           if ( wantSum == 1 ) {
               if ( index(sumList, "," i ",") > 0 ) {
                   printf "%14.2f ", col[i]
               }
           } else {
               printf "%.2f ", col[i]
           }
       }
       printf "\n"
       if (wantSum == 1) {
           printf "Per Query/Op, num qry/ops:  %d\n", numQueries
           for ( i = numDateTimeFields + 1 ; i <= numFields ; i++ ) {
               if ( index(sumList, "," i ",") > 0 ) {
                   printf "%14.2f ", col[i]/numQueries
               }
           }
           printf "\n"
       }
   } else {
       printf "Num Qrs/Ops: %7d  %20s  %20s\n", numQueries, "Total", "Per Qry/Op"
       for ( i = numDateTimeFields + 1 ; i <= numFields ; i++ ) {
           if ( index(sumList, "," i ",") > 0 ) {
               printf "%20s  %20.2f  %20.4f\n", colTtle[i], col[i], col[i]/numQueries
           }
       }
   }
}
