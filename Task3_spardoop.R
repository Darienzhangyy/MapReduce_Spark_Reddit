### Initialization of Rhipe and Hadoop

Sys.setenv(HADOOP="/data/hadoop")
Sys.setenv(HADOOP_HOME="/data/hadoop")
Sys.setenv(HADOOP_BIN="/data/hadoop/bin") 
Sys.setenv(HADOOP_CMD="/data/hadoop/bin/hadoop") 
Sys.setenv(HADOOP_CONF_DIR="/data/hadoop/etc/hadoop") 
Sys.setenv(HADOOP_LIBS=system("/data/hadoop/bin/hadoop classpath | tr -d '*'",TRUE))


if (!("Rhipe" %in% installed.packages()))
{
  install.packages("/data/hadoop/rhipe/Rhipe_0.75.1.6_hadoop-2.tar.gz", repos=NULL)
}

library(Rhipe)
rhinit()

### Word Count

library(tm)
suppressMessages(library(jsonlite))

wc_map_Valen = expression({
  lapply(
    seq_along(map.keys), 
    function(r) 
    { 
      if (as.numeric(fromJSON(map.values[[r]])$created_utc) <= 1423958400 &
          as.numeric(fromJSON(map.values[[r]])$created_utc) >= 1423872000){
      line = tolower(fromJSON(map.values[[r]])$body)
      line = gsub("[-—]"," ",line)
      line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
      line = gsub("(^\\s+|\\s+$)","",line)
      line = strsplit(line, "\\s+")[[1]]
      line = line[line != ""]
      line = tm_map(line, removeWords,stopwords("en"))
      lapply(line, rhcollect, value=1)
      }
    }
  )
})

wc_map_FebSev = expression({
  lapply(
    seq_along(map.keys), 
    function(r) 
    { 
      if (as.numeric(fromJSON(map.values[[r]])$created_utc) <= 1423353600 &
          as.numeric(fromJSON(map.values[[r]])$created_utc) >= 1423267200){
        line = tolower(fromJSON(map.values[[r]])$body)
        line = gsub("[-—]"," ",line)
        line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
        line = gsub("(^\\s+|\\s+$)","",line)
        line = strsplit(line, "\\s+")[[1]]
        line = line[line != ""]
        line = tm_map(line, removeWords,stopwords("en"))
      lapply(line, rhcollect, value=1)
      }
    }
  )
})

wc_map_FebTwoone = expression({
  lapply(
    seq_along(map.keys), 
    function(r) 
    { 
      if (as.numeric(fromJSON(map.values[[r]])$created_utc) <= 1424563200 &
          as.numeric(fromJSON(map.values[[r]])$created_utc) >= 1424476800){
        line = tolower(fromJSON(map.values[[r]])$body)
        line = gsub("[-—]"," ",line)
        line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
        line = gsub("(^\\s+|\\s+$)","",line)
        line = strsplit(line, "\\s+")[[1]]
        line = line[line != ""]
        line = tm_map(line, removeWords,stopwords("en"))
      lapply(line, rhcollect, value=1)
      }
    }
  )
})


wc_reduce = expression(
  pre = {
    total = 0
  },
  reduce = {
    total = total + sum(unlist(reduce.values))
  },
  post = {
    rhcollect(reduce.key, total)
  }
)

wc_Valen = rhwatch(
  map      = wc_map_Valen,
  reduce   = wc_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)

wc_FebSev = rhwatch(
  map      = wc_map_FebSev,
  reduce   = wc_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)

wc_FebTwoone = rhwatch(
  map      = wc_map_FebTwoone,
  reduce   = wc_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)

get_val = function(x,i) x[[i]]

setwd("~/MapReduce")

#Valentine
countsValen = data.frame(key = sapply(wc_Valen,get_val,i=1),
                       value = sapply(wc_Valen,get_val,i=2), 
                       stringsAsFactors=FALSE)
sortValen = countsValen[with(countsValen, order(-value)), ]
Top25Valen = head(sortValen,25)
colnames(Top25Valen) = c("Word","Frequency")
rownames(Top25Valen) <- NULL
save(Top25Valen, file="Top25Valen.RData")
save(sortValen, file="Valentine_Complete.RData")

#Feb 7th
countsFebSev = data.frame(key = sapply(wc_FebSev,get_val,i=1),
                         value = sapply(wc_FebSev,get_val,i=2), 
                         stringsAsFactors=FALSE)
sortFebSev = countsFebSev[with(countsFebSev, order(-value)), ]
Top25FebSev = head(sortFebSev,25)
colnames(Top25FebSev) = c("Word","Frequency")
rownames(Top25FebSev) <- NULL
save(Top25FebSev, file="Top25FebSev.RData")
save(sortFebSev, file="FebSev_Complete.RData")

#Feb 21st
countsFebTwoone = data.frame(key = sapply(wc_FebTwoone,get_val,i=1),
                         value = sapply(wc_FebTwoone,get_val,i=2), 
                         stringsAsFactors=FALSE)
sortFebTwoone = countsFebTwoone[with(countsFebTwoone, order(-value)), ]
Top25FebTwoone = head(sortFebTwoone,25)
colnames(Top25FebTwoone) = c("Word","Frequency")
rownames(Top25FebTwoone) <- NULL
save(Top25FebTwoone, file="Top25FebTwoone.RData")
save(sortFebTwoone, file="FebTwoone_Complete.RData")

#Combine the three data frames
wordCount = rbind(Top25FebSev,Top25Valen,Top25FebTwoone)
colnames(wordCount) = c("Feb7","Frequency7","Feb14","Frequency14",
                             "Feb21","Frequency21")

#barplot of [deleted]
delete7 = Top25FebSev[which(Top25FebSev[,1]=="[deleted]"),2]
delete14 = Top25Valen[which(Top25Valen[,1]=="[deleted]"),2]
delete21 = Top25FebTwoone[which(Top25FebTwoone[,1]=="[deleted]"),2]
barplot(c(delete7,delete14,delete21),names.arg = c("Feb7","Feb14","Feb21"))