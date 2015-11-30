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
counts11$created_utc=as.POSIXct(as.numeric(counts11$created_utc), origin="1970-01-01", tz="UTC")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts11$hour=as.POSIXlt(counts11$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts11$wday=as.POSIXlt(counts11$created_utc)$wday

#aggregate counts by hour
counts_hours_1=aggregate(counts11$count, list(counts11$hour), sum)
#aggregate counts by weekday
counts_wdays_1=aggregate(counts11$count, list(counts11$hour, counts11$wday), sum)



#February counts
counts2 <-summarize(groupBy(df2, df2$created_utc), count = n(df2$created_utc))

counts22=collect(counts2)
#convert created_utc to regular time
counts22$created_utc=as.POSIXct(as.numeric(counts22$created_utc), origin="1970-01-01",tz = "UTC")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts22$hour=as.POSIXlt(counts22$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts22$wday=as.POSIXlt(counts22$created_utc)$wday

#aggregate counts by hour
counts_hours_2=aggregate(counts22$count, list(counts22$hour), sum)
#aggregate counts by weekday
counts_wdays_2=aggregate(counts22$count, list(counts22$hour, counts22$wday), sum)
#merge January counts by hour with February counts by hour
total_hours1=merge(counts_hours_1,counts_hours_2, by="Group.1")
#merge January counts by weekday with February counts by weekday
total_weeks1=cbind(counts_wdays_1[,1:3], counts_wdays_2[,3])

#March counts
counts3 <- summarize(groupBy(df3, df3$created_utc), count = n(df3$created_utc))

counts33=collect(counts3)
#convert created_utc to regular time
counts33$created_utc=as.POSIXct(as.numeric(counts33$created_utc), origin="1970-01-01", tz="UTC")
#subset 'hour' from 'created_utc' and store 'hour' as another column
counts33$hour=as.POSIXlt(counts33$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts33$wday=as.POSIXlt(counts33$created_utc)$wday
#aggregate counts by hour
counts_hours_3=aggregate(counts33$count, list(counts33$hour), sum)
#aggregate counts by weekday
counts_wdays_3=aggregate(counts33$count, list(counts33$hour, counts33$wday), sum)

#April counts
counts4 <- summarize(groupBy(df4, df4$created_utc), count = n(df4$created_utc))

counts44=collect(counts4)
#convert created_utc to regular time
counts44$created_utc=as.POSIXct(as.numeric(counts44$created_utc), origin="1970-01-01", tz="UTC")
#subset 'hour' from 'created_utc' and store 'hour' as another column
counts44$hour=as.POSIXlt(counts44$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts44$wday=as.POSIXlt(counts44$created_utc)$wday
#aggregate counts by hour
counts_hours_4=aggregate(counts44$count, list(counts44$hour), sum)
#aggregate counts by weekday
counts_wdays_4=aggregate(counts44$count, list(counts44$hour, counts44$wday), sum)
#merge March counts by hour with April counts by hour
total_hours2=merge(counts_hours_3,counts_hours_4, by="Group.1")
#merge March counts by weekday with April counts by weekday
total_weeks2=cbind(total_weeks1, counts_wdays_3[,3], counts_wdays_4[,3])


#May counts
counts5 <- summarize(groupBy(df5, df5$created_utc), count = n(df5$created_utc))

counts55=collect(counts5)
#convert created_utc to regular time
counts55$created_utc=as.POSIXct(as.numeric(counts55$created_utc), origin="1970-01-01", tz="UTC")
#subset 'hour' from 'created_utc' and store 'hour' as another column
counts55$hour=as.POSIXlt(counts55$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts55$wday=as.POSIXlt(counts55$created_utc)$wday
#aggregate counts by hour
counts_hours_5=aggregate(counts55$count, list(counts55$hour), sum)
#aggregate counts by weekday
counts_wdays_5=aggregate(counts55$count, list(counts55$hour, counts55$wday), sum)
#merge Jan-Feb counts by hour with Mar-Apr counts by hour
total_hours3=merge(total_hours1, total_hours2, by="Group.1")

#merge Jan-April counts by hour with May counts by hour
CommentsByHour=merge(total_hours3, counts_hours_5, by="Group.1")
#merge Jan-April counts by weekday with May counts by weekday
total_weeks=cbind(total_weeks2, counts_wdays_5[,3])
#sum up Jan-May counts by hour
CommentsByHour$total=rowSums(CommentsByHour[,2:6])
#sum up Jan-May counts by weekday
total_weeks$total=rowSums(total_weeks[,3:7])

colnames(CommentsByHour)=c("Hour", "January", "February", "March", "April", "May", "5 Month Total")

colnames(total_weeks)=c("Hour", "Weekday", "January", "February", "March", "April", "May", "5 Month Total")

#subset comments by weekday
MondayComments=subset(total_weeks, Weekday==1)[,c(1:2,8)]
TuesdayComments=subset(total_weeks, Weekday==2)[,c(1:2,8)]
WednesdayComments=subset(total_weeks, Weekday==3)[,c(1:2,8)]
ThursdayComments=subset(total_weeks, Weekday==4)[,c(1:2,8)]
FridayComments=subset(total_weeks, Weekday==5)[,c(1:2,8)]
SaturdayComments=subset(total_weeks, Weekday==6)[,c(1:2,8)]
SundayComments=subset(total_weeks, Weekday==0)[,c(1:2,8)]



##Gilded

#January Gilded
counts1_gilded=summarize(groupBy(filter(df1, df1$gilded!=0), df1$created_utc),count = n(df1$created_utc))
counts11_gilded=collect(counts1_gilded)
#convert created_utc to regular time
counts11_gilded$created_utc=as.POSIXct(as.numeric(counts11_gilded$created_utc), origin="1970-01-01", tz="UTC")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts11_gilded$hour=as.POSIXlt(counts11_gilded$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts11_gilded$wday=as.POSIXlt(counts11_gilded$created_utc)$wday
#aggregate counts by hour
counts_hours_1_gilded=aggregate(counts11_gilded$count, list(counts11_gilded$hour), sum)
#aggregate counts by weekday
counts_wdays_1_gilded=aggregate(counts11_gilded$count, list(counts11_gilded$hour,counts11_gilded$wday), sum)

#February Gilded
counts2_gilded=summarize(groupBy(filter(df2, df2$gilded!=0), df2$created_utc),count = n(df2$created_utc))
counts22_gilded=collect(counts2_gilded)

#convert created_utc to regular time
counts22_gilded$created_utc=as.POSIXct(as.numeric(counts22_gilded$created_utc), origin="1970-01-01", tz="UTC")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts22_gilded$hour=as.POSIXlt(counts22_gilded$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts22_gilded$wday=as.POSIXlt(counts22_gilded$created_utc)$wday
#aggregate counts by hour
counts_hours_2_gilded=aggregate(counts22_gilded$count, list(counts22_gilded$hour), sum)
#aggregate counts by weekday
counts_wdays_2_gilded=aggregate(counts22_gilded$count, list(counts22_gilded$hour,counts22_gilded$wday), sum)
#merge January counts by hour with February counts by hour
total_hours1_gilded=merge(counts_hours_1_gilded,counts_hours_2_gilded, by="Group.1")
#merge January counts by weekday with February counts by weekday
total_weeks1_gilded=cbind(counts_wdays_1_gilded, counts_wdays_2_gilded[,3])

#March Gilded
counts3_gilded=summarize(groupBy(filter(df3, df3$gilded!=0), df3$created_utc),count = n(df3$created_utc))
counts33_gilded=collect(counts3_gilded)
#convert created_utc to regular time
counts33_gilded$created_utc=as.POSIXct(as.numeric(counts33_gilded$created_utc), origin="1970-01-01", tz="UTC")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts33_gilded$hour=as.POSIXlt(counts33_gilded$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts33_gilded$wday=as.POSIXlt(counts33_gilded$created_utc)$wday
#aggregate counts by hour
counts_hours_3_gilded=aggregate(counts33_gilded$count, list(counts33_gilded$hour), sum)
#aggregate counts by weekday
counts_wdays_3_gilded=aggregate(counts33_gilded$count, list(counts33_gilded$hour,counts33_gilded$wday), sum)

#April Gilded
counts4_gilded=summarize(groupBy(filter(df4, df4$gilded!=0), df4$created_utc),count = n(df4$created_utc))
counts44_gilded=collect(counts4_gilded)
#convert created_utc to regular time
counts44_gilded$created_utc=as.POSIXct(as.numeric(counts44_gilded$created_utc), origin="1970-01-01", tz="UTC")

#subset 'hour' from 'created_utc' and store 'hour' as another column
counts44_gilded$hour=as.POSIXlt(counts44_gilded$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts44_gilded$wday=as.POSIXlt(counts44_gilded$created_utc)$wday
#aggregate counts by hour
counts_hours_4_gilded=aggregate(counts44_gilded$count, list(counts44_gilded$hour), sum)
#aggregate counts by weekday
counts_wdays_4_gilded=aggregate(counts44_gilded$count, list(counts44_gilded$hour,counts44_gilded$wday), sum)
#merge January counts by hour with February counts by hour
total_hours2_gilded=merge(counts_hours_3_gilded,counts_hours_4_gilded, by="Group.1")
#merge January counts by weekday with February counts by weekday
total_weeks2_gilded=cbind(total_weeks1_gilded, counts_wdays_3_gilded[,3], counts_wdays_4_gilded[,3])

#May Gilded
counts5_gilded<- summarize(groupBy(filter(df5, df5$gilded!=0),df5$created_utc), count = n(df5$created_utc))

counts55_gilded=collect(counts5_gilded)
#convert created_utc to regular time
counts55_gilded$created_utc=as.POSIXct(as.numeric(counts55_gilded$created_utc), origin="1970-01-01", tz="UTC")
#subset 'hour' from 'created_utc' and store 'hour' as another column
counts55_gilded$hour=as.POSIXlt(counts55_gilded$created_utc)$hour
#subset 'wday' from 'created_utc' and store 'wday' as another column
counts55_gilded$wday=as.POSIXlt(counts55_gilded$created_utc)$wday
#aggregate counts by hour
counts_hours_5_gilded=aggregate(counts55_gilded$count, list(counts55_gilded$hour), sum)
#aggregate counts by weekday
counts_wdays_5_gilded=aggregate(counts55_gilded$count, list(counts55_gilded$hour,counts55_gilded$wday), sum)
#merge Jan-Feb counts by hour with Mar-Apr counts by hour
total_hours3_gilded=merge(total_hours1_gilded, total_hours2_gilded, by="Group.1")
#merge Jan-April counts by hour with May counts by hour
CommentsByHour_G=merge(total_hours3_gilded, counts_hours_5_gilded, by="Group.1")
#merge Jan-April counts by weekday with May counts by weekday
total_weeks_G=cbind(total_weeks2_gilded, counts_wdays_5_gilded[,3])
#sum up Jan-May counts by hour
CommentsByHour_G$total=rowSums(CommentsByHour_G[,2:6])
#sum up Jan-May counts by weekday
total_weeks_G$total=rowSums(total_weeks_G[,3:7])

colnames(CommentsByHour_G)=c("Hour", "January", "February", "March", "April", "May", "5 Month Total")

colnames(total_weeks_G)=c("Hour", "Weekday", "January", "February", "March", "April", "May", "5 Month Total")

#subset comments by weekday
MondayComments_G=subset(total_weeks_G, Weekday==1)[,c(1:2,8)]
TuesdayComments_G=subset(total_weeks_G, Weekday==2)[,c(1:2,8)]
WednesdayComments_G=subset(total_weeks_G, Weekday==3)[,c(1:2,8)]
ThursdayComments_G=subset(total_weeks_G, Weekday==4)[,c(1:2,8)]
FridayComments_G=subset(total_weeks_G, Weekday==5)[,c(1:2,8)]
SaturdayComments_G=subset(total_weeks_G, Weekday==6)[,c(1:2,8)]
SundayComments_G=subset(total_weeks_G, Weekday==0)[,c(1:2,8)]

#save all data frames into hourly.RData
save(list=c("CommentsByHour","CommentsByHour_G","MondayComments","MondayComments_G","TuesdayComments",
            "TuesdayComments_G","WednesdayComments","WednesdayComments_G","ThursdayComments","ThursdayComments_G",
            "FridayComments", "FridayComments_G","SaturdayComments","SaturdayComments_G","SundayComments",
            "SundayComments_G" ), file="hourly.RData")
