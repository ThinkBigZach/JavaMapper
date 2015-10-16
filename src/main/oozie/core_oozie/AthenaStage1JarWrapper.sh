#!/bin/bash

SOURCE_PATH=$0
ENTITY=$1
OUT_PATH=$2
PRACTICE_MAP=$3
ENTITY_MAP=$4
TD_HOST=$5
TD_USER=$6
TD_PASSWORD=$7
TD_DATABASE=$8
DIV_FLAG=$9
DIVISIONS_MAP=${10}

Division_ids=`hadoop fs -cat $DIVISIONS_MAP`
size=`echo "$Division_ids" | wc -l`
newlist=()
hadoop fs -copyToLocal hdfs:///user/athena/Athena1ETL.jar /home/athena/scripts/testing1/Athena1ETL.jar

for id in $Division_ids 
do
TEMP_PATH1=`echo "$SOURCE_PATH" | cut -d"/" -f 1-4`
TEMP_PATH2=`echo "$SOURCE_PATH" | cut -d"/" -f 6-`
NEW_SOURCE="$TEMP_PATH1/$id/$TEMP_PATH2"
newlist+=('`hadoop jar /user/athena/scripts/testing1/Athena1ETL.jar $NEW_SOURCE $ENTITY $OUT_PATH $PRACTICE_MAP $ENTITY_MAP $TD_HOST $TD_USER $TD_PASSWORD $TD_DATABASE $DIV_FLAG &`')

done
wait

i=0
for val in $newlist
do

if [ -n $val ] ;
then
i=$(( $i + 1 ))
fi
done

if [ $i -gt 0 ] ;
then
echo "returnCode=FAILED"
else
echo "returnCode=SUCCESS"
fi
