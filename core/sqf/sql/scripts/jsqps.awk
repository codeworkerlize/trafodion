# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2018 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@

BEGIN {
    lv_header_line_number = 0;
    lv_lines_since_header = 0;
    lv_first = 1;
    lv_got_data = 0;
}

{
    if (lv_header_line_number > 0) {
        lv_lines_since_header = FNR - lv_header_line_number;
        if (lv_lines_since_header > 1) {
           if (lv_first == 1) {
                lv_first = 0;
                printf("[");
                lv_got_data = 1;
           }
           else {
                printf(",");
           }
           split($2,a,",");
           printf "{\"Host\": \"%s\", \"ProcessID\": \"%s\", \"ProcessName\": \"%s\", \"Parent\": \"%s\", \"Program\": \"%s\"}",
                   a[1], a[2], $6, $7, $8
        }
    }
}

/ NID,PID/ {lv_header_line_number = FNR;}

END {
    if (lv_got_data == 1){
        printf("]\n");
    }
}

