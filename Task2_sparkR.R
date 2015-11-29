Sys.setenv(HADOOP_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(YARN_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(SPARK_HOME="/data/hadoop/spark")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R/lib"), .libPaths()))
library(SparkR)


sc = sparkR.init(master="yarn-client")
sqlContext = sparkRSQL.init(sc)


df1<- read.df(sqlContext, "hdfs://localhost:9000/data/RC_2015-01.json", source = "json")
df2<- read.df(sqlContext, "hdfs://localhost:9000/data/RC_2015-02.json", source = "json")
df3<- read.df(sqlContext, "hdfs://localhost:9000/data/RC_2015-03.json", source = "json")
df4<- read.df(sqlContext, "hdfs://localhost:9000/data/RC_2015-04.json", source = "json")
df5<- read.df(sqlContext, "hdfs://localhost:9000/data/RC_2015-05.json", source = "json")

#January counts
counts1=summarize(groupBy(df1, df1$created_utc), count = n(df1$created_utc))

counts11=collect(counts1)
#convert created_utc to regular time
counts11$created_utc=as.POSIXct(as.numeric(counts11$created_utc), origin="1970-01-01")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts11$hour=as.POSIXlt(counts11$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts11$wday=as.POSIXlt(counts11$created_utc)$wday

#aggregate counts by hour
counts_hours_1=aggregate(counts11$count, list(counts11$hour), sum)
#aggregate counts by weekday
counts_wdays_1=aggregate(counts11$count, list(counts11$wday), sum)



#February counts
counts2 <- summarize(groupBy(df2, df2$created_utc), count = n(df2$created_utc))

counts22=collect(counts2)
#convert created_utc to regular time
counts22$created_utc=as.POSIXct(as.numeric(counts22$created_utc), origin="1970-01-01")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts22$hour=as.POSIXlt(counts22$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts22$wday=as.POSIXlt(counts22$created_utc)$wday
#aggregate counts by hour
counts_hours_2=aggregate(counts22$count, list(counts22$hour), sum)
#aggregate counts by weekday
counts_wdays_2=aggregate(counts22$count, list(counts22$wday), sum)
#merge January counts by hour with February counts by hour
total_hours1=merge(counts_hours_1,counts_hours_2, by="Group.1")
#merge January counts by weekday with February counts by weekday
total_weeks1=merge(counts_wdays_1, counts_wdays_2, by="Group.1")

#March counts
counts3 <- summarize(groupBy(df3, df3$created_utc), count = n(df3$created_utc))

counts33=collect(counts3)
#convert created_utc to regular time
counts33$created_utc=as.POSIXct(as.numeric(counts33$created_utc), origin="1970-01-01")
#subset 'hour' from 'created_utc' and store 'hour' as another column
counts33$hour=as.POSIXlt(counts33$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts33$wday=as.POSIXlt(counts33$created_utc)$wday
#aggregate counts by hour
counts_hours_3=aggregate(counts33$count, list(counts33$hour), sum)
#aggregate counts by weekday
counts_wdays_3=aggregate(counts33$count, list(counts33$wday), sum)

#April counts
counts4 <- summarize(groupBy(df4, df4$created_utc), count = n(df4$created_utc))

counts44=collect(counts4)
#convert created_utc to regular time
counts44$created_utc=as.POSIXct(as.numeric(counts44$created_utc), origin="1970-01-01")
#subset 'hour' from 'created_utc' and store 'hour' as another column
counts44$hour=as.POSIXlt(counts44$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts44$wday=as.POSIXlt(counts44$created_utc)$wday
#aggregate counts by hour
counts_hours_4=aggregate(counts44$count, list(counts44$hour), sum)
#aggregate counts by weekday
counts_wdays_4=aggregate(counts44$count, list(counts44$wday), sum)
#merge March counts by hour with April counts by hour
total_hours2=merge(counts_hours_3,counts_hours_4, by="Group.1")
#merge March counts by weekday with April counts by weekday
total_weeks2=merge(counts_wdays_3, counts_wdays_4, by="Group.1")


#May counts
counts5 <- summarize(groupBy(df5, df5$created_utc), count = n(df5$created_utc))

counts55=collect(counts5)
#convert created_utc to regular time
counts55$created_utc=as.POSIXct(as.numeric(counts55$created_utc), origin="1970-01-01")
#subset 'hour' from 'created_utc' and store 'hour' as another column
counts55$hour=as.POSIXlt(counts55$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts55$wday=as.POSIXlt(counts55$created_utc)$wday
#aggregate counts by hour
counts_hours_5=aggregate(counts55$count, list(counts55$hour), sum)
#aggregate counts by weekday
counts_wdays_5=aggregate(counts55$count, list(counts55$wday), sum)
#merge Jan-Feb counts by hour with Mar-Apr counts by hour
total_hours3=merge(total_hours1, total_hours2, by="Group.1")
#merge Jan-Feb counts by week with Mar-Apr counts by week
total_weeks3=merge(total_weeks1, total_weeks2, by="Group.1")
#merge Jan-April counts by hour with May counts by hour
total_hours=merge(total_hours3, counts_hours_5, by="Group.1")
#merge Jan-April counts by weekday with May counts by weekday
total_weeks=merge(total_weeks3, counts_wdays_5, by="Group.1")
#sum up Jan-May counts by hour
total_hours$total=rowSums(total_hours[,2:6])
#sum up Jan-May counts by weekday
total_weeks$total=rowSums(total_weeks[,2:6])

jpeg('Freq_comments_Jan-May.jpeg')
plot(total_hours$Group.1, total_hours$total, xlab="Hour", ylab="Frequency of Comments")
dev.off()

jpeg('Freq_comments_weekly.jpeg')
plot(total_weeks$Group.1, total_weeks$total, xlab="Weekday", ylab="Frequency of Comments")
dev.off()





