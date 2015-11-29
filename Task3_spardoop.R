### Word Count

wc_map = expression({
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      line = tolower(map.values[[r]])$body
      line = gsub("[-—]"," ",line)
      line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
      line = gsub("(^\\s+|\\s+$)","",line)
      line = strsplit(line, "\\s+")[[1]]
      line = line[line != ""]
      
      lapply(line, rhcollect, value=1)
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
  map      = wc_map,
  reduce   = wc_reduce,
  input    = rhfmt("/data/Shakespeare/hamlet.txt", type = "text")
)

wc_FebSev = rhwatch(
  map      = wc_map,
  reduce   = wc_reduce,
  input    = rhfmt("/data/Shakespeare/hamlet.txt", type = "text")
)

wc_FebTwoone = rhwatch(
  map      = wc_map,
  reduce   = wc_reduce,
  input    = rhfmt("/data/Shakespeare/hamlet.txt", type = "text")
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