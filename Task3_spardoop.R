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

## Uncomment following lines if you need non-base packages
rhoptions(zips = '/R/R.Pkg.tar.gz')
rhoptions(runner = 'sh ./R.Pkg/library/Rhipe/bin/RhipeMapReduce.sh')

### Word Count

# define a map function for Valentine's day
wc_map_Valen = expression({
  suppressMessages(library(jsonlite))
  #suppressMessages(library(tm))
  lapply(
    seq_along(map.keys), 
    function(r) 
    { 
      #clean data and remove stopwords
      if (as.numeric(fromJSON(map.values[[r]])$created_utc) <= 1423958400 &
          as.numeric(fromJSON(map.values[[r]])$created_utc) >= 1423872000){
      line = tolower(fromJSON(map.values[[r]])$body)
      line = gsub("[-—]"," ",line)
      line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
      line = gsub("(^\\s+|\\s+$)","",line)
      line = strsplit(line, "\\s+")[[1]]
      line = line[line != ""]
      #line = tm_map(line, removeWords,stopwords("en"))
      lapply(line, rhcollect, value=1)
      }
    }
  )
})
# define a map function for February 7th 
wc_map_FebSev = expression({
  suppressMessages(library(jsonlite))
  #suppressMessages(library(tm))
  lapply(
    seq_along(map.keys), 
    function(r) 
    { 
      #clean data and remove stopwords
      if (as.numeric(fromJSON(map.values[[r]])$created_utc) <= 1423353600 &
          as.numeric(fromJSON(map.values[[r]])$created_utc) >= 1423267200){
        line = tolower(fromJSON(map.values[[r]])$body)
        line = gsub("[-—]"," ",line)
        line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
        line = gsub("(^\\s+|\\s+$)","",line)
        line = strsplit(line, "\\s+")[[1]]
        line = line[line != ""]
        #line = tm_map(line, removeWords,stopwords("en"))
      lapply(line, rhcollect, value=1)
      }
    }
  )
})

# define a map function for February 21th 
wc_map_FebTwoone = expression({
  suppressMessages(library(jsonlite))
  #suppressMessages(library(tm))
  lapply(
    seq_along(map.keys), 
    function(r) 
    { 
      #clean data and remove stopwords
      if (as.numeric(fromJSON(map.values[[r]])$created_utc) <= 1424563200 &
          as.numeric(fromJSON(map.values[[r]])$created_utc) >= 1424476800){
        line = tolower(fromJSON(map.values[[r]])$body)
        line = gsub("[-—]"," ",line)
        line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
        line = gsub("(^\\s+|\\s+$)","",line)
        line = strsplit(line, "\\s+")[[1]]
        line = line[line != ""]
        #line = tm_map(line, removeWords,stopwords("en"))
      lapply(line, rhcollect, value=1)
      }
    }
  )
})

# define a reduce function 
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

# creat a R object wc_Valen to count words on Valentine's day
wc_Valen = rhwatch(
  map      = wc_map_Valen,
  reduce   = wc_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)

# creat a R object wc_FebSev to count words on Valentine's day
wc_FebSev = rhwatch(
  map      = wc_map_FebSev,
  reduce   = wc_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)

# creat a R object wc_FebTwoone to count words on Valentine's day
wc_FebTwoone = rhwatch(
  map      = wc_map_FebTwoone,
  reduce   = wc_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)

get_val = function(x,i) x[[i]]

setwd("~/MapReduce")

#Valentine
stopword = stopwords("en")
#call the MapReduce function for Valentine's day
countsValen = data.frame(key = sapply(wc_Valen,get_val,i=1),
                       value = sapply(wc_Valen,get_val,i=2), 
                       stringsAsFactors=FALSE)
sortValen = countsValen[with(countsValen, order(-value)), ]
#sort words from counting result
sortValen = sortValen[which(!(sortValen$key %in% stopword)),]
#take out Top25 words
Top25Valen = head(sortValen,25)
colnames(Top25Valen) = c("Word","Frequency")
rownames(Top25Valen) <- NULL
#save ranking result
save(Top25Valen, file="Top25Valen.RData")
save(sortValen, file="Valentine_Complete.RData")

#Feb 7th
stopword = stopwords("en")
#call the MapReduce function for February 7th
countsFebSev = data.frame(key = sapply(wc_FebSev,get_val,i=1),
                         value = sapply(wc_FebSev,get_val,i=2), 
                         stringsAsFactors=FALSE)
#sort words from counnting result
sortFebSev = countsFebSev[with(countsFebSev, order(-value)), ]
sortFebSev = sortFebSev[which(!(sortFebSev$key %in% stopword)),]
#take out Top25 words
Top25FebSev = head(sortFebSev,25)
colnames(Top25FebSev) = c("Word","Frequency")
rownames(Top25FebSev) <- NULL
#save ranking result
save(Top25FebSev, file="Top25FebSev.RData")
save(sortFebSev, file="FebSev_Complete.RData")

#Feb 21st
stopword = stopwords("en")
#call the MapReduce function for February 21th
countsFebTwoone = data.frame(key = sapply(wc_FebTwoone,get_val,i=1),
                         value = sapply(wc_FebTwoone,get_val,i=2), 
                         stringsAsFactors=FALSE)
sortFebTwoone = countsFebTwoone[with(countsFebTwoone, order(-value)), ]
#sort words from counnting result
sortFebTwoone = sortFebTwoone[which(!(sortFebTwoone$key %in% stopword)),]
#take out Top25 words
Top25FebTwoone = head(sortFebTwoone,25)
colnames(Top25FebTwoone) = c("Word","Frequency")
rownames(Top25FebTwoone) <- NULL
#save ranking result
save(Top25FebTwoone, file="Top25FebTwoone.RData")
save(sortFebTwoone, file="FebTwoone_Complete.RData")

#Combine the three data frames
wordCount = cbind(Top25FebSev[,1:2],Top25Valen[,1:2],Top25FebTwoone[,1:2])
colnames(wordCount) = c("Feb7","Frequency7","Feb14","Frequency14",
                             "Feb21","Frequency21")
save(wordCount, file="Feb_ThreeDays.RData")

#barplot of [deleted]
delete7 = Top25FebSev[which(Top25FebSev[,1]=="deleted"),2]
delete14 = Top25Valen[which(Top25Valen[,1]=="deleted"),2]
delete21 = Top25FebTwoone[which(Top25FebTwoone[,1]=="deleted"),2]
barplot(c(delete7,delete14,delete21),names.arg = c("Feb7","Feb14","Feb21"))
