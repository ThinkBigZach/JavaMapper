#!/bin/bash
if [ "$#" -ne 2 ]; then
	echo "ERROR: wrong # of args"
	echo "returnCode=FAILED"
	exit 1
else
	uID=$USER
	MODE=${1}
	dataPartID=${2}
	entity="*"
	if [ ${uID} != "athena" ]; then
		echo "ERROR: must be athena USER to run"
		echo "returnCode=FAILED"
		exit 1
	else
		COORD_FILE=CURRENT_OOZIE_COORDINATOR_${MODE}_${dataPartID}_${srcSystemID}
		if [  -f ./${COORD_FILE} ]; then
			jobID=$(cat ./${COORD_FILE})
			oozie job -oozie http://10.1.132.20:11000/oozie -suspend ${jobID} 
			oozie job -oozie http://10.1.132.20:11000/oozie -kill  ${jobID}
			rm -f ./${COORD_FILE}  ;
		fi
	#fi
	lowermode="unset"
	if [ "$MODE" == "PROD" ]; then
		stage1UserID="athena"
		lowermode="prod"
		tdServer=prod.teradata.chs.net
		tdUserIDPassword=inf0rmt1prod3t1
	elif [ "$MODE" ==  "DEV" ]; then
    	stage1UserID="athena"
    	lowerMode="dev"
    	tdServer=dev.teradata.chs.net
    	tdUserID=dbc
    	tdUserIDPassword=dbc
	else
		echo "returnCode=NOOP"
		exit -1
	fi

	rawJobID=$(oozie job -oozie http://10.1.132.20:11000/oozie -config "/hdfs_mount/user/${uID}/stage-3-oozie/job.properties" -DcoordStart=`date -u "+%Y-%m-%dT%H:00Z"` -DuserName=${uID} -DstageThreeOwner=${stage1UserID} -DstageThreeDataPartition=${dataPartID} -DtdServer=${tdServer} -DtdUserID=${tdUserID} -DtdUserIDPassword=${tdUserIDPassword} -Dentity=${entity} -submit)

	newJobID=$( echo ${rawJobID} | awk '{print $2}')
	echo $newJobID > ./${COORD_FILE}
	fi
fi
echo "returnCode=${newJobID}"
exit 0
