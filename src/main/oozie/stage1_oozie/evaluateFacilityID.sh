#!/bin/bash
if [ "$#" -ne 3 ]; then
        echo "ERROR: wrong # of args"
       	echo "expected [facilityID] [divisional map file path] [sourcepath]"
        echo "returnCode=FAILED"
        exit 1
fi

facilityID=$1

if [ "$facilityID" -eq "9999" ] ; then
echo "runType=divisional,runSource=$3"
exit 0

else
runSource=`echo "$3" | sed "s/\*/$facilityID/g"`
divs=`hadoop fs -cat $2`

for div in $divs 
do
	if [ "$facilityID" -eq "$div" ] ; then
	echo "runType=divisional,runSource=$runSource"
	exit 0
	else
	echo "runType=path,runSource=$runSource"
	exit 0
	fi
done
fi
