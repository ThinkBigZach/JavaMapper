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
			oozie job -oozie http://10.1.132.24:11000/oozie -suspend ${jobID} 
			oozie job -oozie http://10.1.132.24:11000/oozie -kill  ${jobID}
			rm -f ./${COORD_FILE}  ;
		fi
	#fi
	lowermode="unset"
	if [ "$MODE" == "PROD" ]; then
		custommapUserID="athena"
		lowermode="prod"
		tdServer=prod.teradata.chs.net
		tdUserID=athena
		tdUserIDPassword=tdchs123
	elif [ "$MODE" ==  "DEV" ]; then
    	custommapUserID="athena"
    	lowerMode="dev"
    	tdServer=dev.teradata.chs.net
		tdUserID=dbc
    	tdUserIDPassword=dbc
	else
		echo "returnCode=NOOP"
		exit -1
	fi

	hadoop fs -rm -skipTrash /user/${uID}/data/${dataPartID}/oozie/job.properties
	hadoop fs -rm -skipTrash /user/${uID}/data/${dataPartID}/oozie/coordinator.xml
	hadoop fs -rm -skipTrash /user/${uID}/data/${dataPartID}/oozie/workflow.xml

	hadoop fs -put job.properties /user/${uID}/data/${dataPartID}/oozie/job.properties
	hadoop fs -put coordinator.xml /user/${uID}/data/${dataPartID}/oozie/coordinator.xml
	hadoop fs -put workflow.xml /user/${uID}/data/${dataPartID}/oozie/workflow.xml

	rawJobID=$(oozie job -oozie http://10.1.132.24:11000/oozie -config "/hdfs_mount/user/${uID}/custommap-oozie/job.properties" -DcoordStart=`date -u "+%Y-%m-%dT%H:00Z"` -DuserName=${uID} -Dstage3CustomMapOwner=${custommapUserID} -DcustomMapDataPartition=${dataPartID} -DtdServer=${tdServer} -DtdUserID=${tdUserID} -DtdUserIDPassword=${tdUserIDPassword} -Dentity=${entity} -submit)

	newJobID=$( echo ${rawJobID} | awk '{print $2}')
	echo $newJobID > ./${COORD_FILE}
	fi
fi
echo "returnCode=${newJobID}"
exit 0
