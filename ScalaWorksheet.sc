
#Show tables in Spark shell -
#=============================
sqlContext.tables().filter("tableName LIKE '%sessions%'").collect()
