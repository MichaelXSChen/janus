#!/bin/bash

cat log/proc-host* | grep "time to" | grep COM | cut -d ' ' -f 10 > log/ttCOM
cat log/proc-host* | grep "time to" | grep DIS | cut -d ' ' -f 10 > log/ttDIS
cat log/proc-host* | grep "time to" | grep PRE | cut -d ' ' -f 10 > log/ttPRE
cat log/proc-host* | grep "time to" | grep FIRST_PR | cut -d ' ' -f 9 > log/ttFIRST_PR
cat log/proc-host* | grep "time to" | grep TOTAL_PR | cut -d ' ' -f 9 > log/ttTOTAL_PR
