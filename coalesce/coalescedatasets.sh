COALESCE_LOGNAME="/data/logs/coalesce.log"

rm $COALESCE_LOGNAME

echo "######## Staring HDFS Coalesce ############## " + $(date + "%m_%d_%Y_%H_%M_%S") > $COALESCE_LOGNAME

spark2-shell --master yarn --queue root.queue --driver-memory 16g --executor-memory 16g --conf spark.ui.port=39998 --conf spark.driver.extraJavaOptions='-Dconfig.file=/etc/security/certs/abcROOTCA.pem' -i /apps/HDFSCoalesce/performcoalesce.scala >> $COALESCE_LOGNAME

err_cnt = `egrep -i 'error|Exception:' $COALESCE_LOGNAME | grep -v 'START Processing File >>' | egrep -v 'java.io.FileNotFoundException:|is not a directory' | wc -l`

if [ $err_cnt -gt 0 ]
then
    echo "Errors present in coalesce log file. Please check log file for more details."
    exit 1
else
    echo "Errors not present in coalesce log file"
    exit 0
fi