EMAIL_ADDRESS="abc@pqr.com"
COALESCE_LOGNAME="/data/logs/listcoalescecandidates.log"
COALESCE_REPORT_MAIL_BODY="/data/logs/coalescereportemailbody.txt"

rm $COALESCE_LOGNAME
rm $COALESCE_REPORT_MAIL_BODY

echo "######## Staring HDFS List Coalesce Candidates ############## " + $(date + "%m_%d_%Y_%H_%M_%S") > $COALESCE_LOGNAME

spark2-shell --master yarn --queue root.queue --driver-memory 8g --executor-memory 8g --conf spark.ui.port=39998 --conf spark.driver.extraJavaOptions='-Dconfig.file=/etc/security/certs/abcROOTCA.pem' -i /apps/HDFSCoalesce/listcoalescecandidates.scala >> $COALESCE_LOGNAME


echo "######## Completed HDFS List Coalesce Candidates ############## " + $(date + "%m_%d_%Y_%H_%M_%S") > $COALESCE_LOGNAME
sleep 5m

compchk=`grep -c "Completed HDFS List Coalesce for Datasets" $COALESCE_LOGNAME`

if [ $compchk != 1 ]
then
    echo "Scala script did not complete by --> " + $(date + "%m_%d_%Y_%H_%M_%S")
    exit 1
fi

echo "Started reporting of coalesce candidates list : " + $(date + "%m_%d_%Y_%H_%M_%S")

rptname=/data/logs/listcoalescecandidates_$(date + "%m_%d_%Y_%H_%M_%S").csv
echo "Dataset Name"",""Metadata Files Count" > $rptname

grep 'START Processing File >>' $COALESCE_LOGNAME | sed 's/START Processing File >>//g' | sort --field-separator=',' -n -r -k 2,1 >> $rptname

rptcnt=`cat $rptname | grep -v -w "Dataset Name, Metadata Files Count" | wc -l`
echo $rptcnt

if [ $rptcnt -le 0 ]
then
    noofdatasets=no
else
    noofdatasets=$rptcnt
fi

echo "There are $noofdatasets datasets eligible for coalesce maintenance activity" > $COALESCE_REPORT_MAIL_BODY

mailx -a $rptname -s "Datasets Coalesce Candidates Report - $(date  + "%m-%d-%Y")" $EMAIL_ADDRESS < $COALESCE_REPORT_MAIL_BODY


err_cnt = `egrep -i 'error|Exception:' $COALESCE_LOGNAME | grep -v 'START Processing File >>' | egrep -v 'java.io.FileNotFoundException:|is not a directory' | wc -l`

if [ $err_cnt -gt 0 ]
then
    echo "Errors present in coalesce log file. Please check log file for more details."
    exit 1
else
    echo "Errors not present in coalesce log file"
    exit 0
fi