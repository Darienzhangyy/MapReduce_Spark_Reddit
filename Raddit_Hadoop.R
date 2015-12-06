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

### top 25 subreddits

# define a map function to rank subreddit
Red_map = expression({
  suppressMessages(library(jsonlite))

  lapply(
    seq_along(map.keys),
    function(r)
    {
      key = fromJSON(map.values[[r]])$subreddit
      value = 1
      rhcollect(key,value)
    }
  )
})


# define a corresponding reduce function
Red_reduce = expression(
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


# TPsubred = rhwatch(
#   map      = Red_map,
#   reduce   = Red_reduce,
#   input    = rhfmt("/data/short_1e3.json", type = "text")
# )

# creat a R object TPsubredJan to rank subreddit in January
TPsubredJan = rhwatch(
  map      = Red_map,
  reduce   = Red_reduce,
  input    = rhfmt("/data/RC_2015-01.json", type = "text")
)

# creat a R object TPsubredJan to rank subreddit in February
TPsubredFeb = rhwatch(
  map      = Red_map,
  reduce   = Red_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)

# creat a R object TPsubredJan to rank subreddit in March
TPsubredMar = rhwatch(
  map      = Red_map,
  reduce   = Red_reduce,
  input    = rhfmt("/data/RC_2015-03.json", type = "text")
)

# creat a R object TPsubredJan to rank subreddit in April
TPsubredApr = rhwatch(
  map      = Red_map,
  reduce   = Red_reduce,
  input    = rhfmt("/data/RC_2015-04.json", type = "text")
)

# creat a R object TPsubredJan to rank subreddit in May
TPsubredMay = rhwatch(
  map      = Red_map,
  reduce   = Red_reduce,
  input    = rhfmt("/data/RC_2015-05.json", type = "text")
)



get_val = function(x,i) x[[i]]

#data frame of all the subreddit count
#Save data
setwd("~/MapReduce")

#Jan
countsJan = data.frame(key = sapply(TPsubredJan,get_val,i=1),
                    value = sapply(TPsubredJan,get_val,i=2),
                    stringsAsFactors=FALSE)
sortJan = countsJan[with(countsJan, order(-value)), ]
sortJan$rank = seq.int(nrow(sortJan))
Top25Jan = head(sortJan,25)
colnames(Top25Jan) = c("Subreddit","This month", "This month's rank")
rownames(Top25Jan) <- NULL
save(Top25Jan, file="Top25Jan.RData")
save(sortJan, file="Jan_Complete.RData")

#Feb
countsFeb = data.frame(key = sapply(TPsubredFeb,get_val,i=1),
                       value = sapply(TPsubredFeb,get_val,i=2),
                       stringsAsFactors=FALSE)
sortFeb = countsFeb[with(countsFeb, order(-value)), ]
sortFeb$rank = seq.int(nrow(sortFeb))
Top25Feb = head(sortFeb,25)
#Compare with the words in the previous month
Top25Feb = merge(Top25Feb,sortJan,by = "key",all.x = T,all.y = F)
Top25Feb = Top25Feb[with(Top25Feb, order(-value.x)), ]
colnames(Top25Feb) = c("Subreddit","This month", "This month's rank",
                       "Last month","Last month's rank")
Top25Feb$`Rank change` = Top25Feb$`Last month's rank`-Top25Feb$`This month's rank`
#reorder the column sequence
Top25Feb = Top25Feb[c(1,2,4,3,5,6)]
rownames(Top25Feb) <- NULL
save(Top25Feb, file="Top25Feb.RData")
save(sortFeb, file="Feb_Complete.RData")

#Mar
countsMar = data.frame(key = sapply(TPsubredMar,get_val,i=1),
                       value = sapply(TPsubredMar,get_val,i=2),
                       stringsAsFactors=FALSE)
sortMar = countsMar[with(countsMar, order(-value)), ]
sortMar$rank = seq.int(nrow(sortMar))
Top25Mar = head(sortMar,25)
#Compare with the words in the previous month
Top25Mar = merge(Top25Mar,sortFeb,by = "key",all.x = T,all.y = F)
Top25Mar = Top25Mar[with(Top25Mar, order(-value.x)), ]
colnames(Top25Mar) = c("Subreddit","This month", "This month's rank",
                       "Last month","Last month's rank")
Top25Mar$`Rank change` = Top25Mar$`Last month's rank`-Top25Mar$`This month's rank`
#reorder the column sequence
Top25Mar = Top25Mar[c(1,2,4,3,5,6)]
rownames(Top25Mar) <- NULL
save(Top25Mar, file="Top25Mar.RData")
save(sortMar, file="Mar_Complete.RData")

#Apr
countsApr = data.frame(key = sapply(TPsubredApr,get_val,i=1),
                       value = sapply(TPsubredApr,get_val,i=2),
                       stringsAsFactors=FALSE)
sortApr = countsApr[with(countsApr, order(-value)), ]
sortApr$rank = seq.int(nrow(sortApr))
Top25Apr = head(sortApr,25)
#Compare with the words in the previous month
Top25Apr = merge(Top25Apr,sortMar,by = "key",all.x = T,all.y = F)
Top25Apr = Top25Apr[with(Top25Apr, order(-value.x)), ]
colnames(Top25Apr) = c("Subreddit","This month", "This month's rank",
                       "Last month","Last month's rank")
Top25Apr$`Rank change` = Top25Apr$`Last month's rank`-Top25Apr$`This month's rank`
#reorder the column sequence
Top25Apr = Top25Apr[c(1,2,4,3,5,6)]
rownames(Top25Apr) <- NULL
Top25Apr[is.na(Top25Apr)] <- "--"
save(Top25Apr, file="Top25Apr.RData")
save(sortApr, file="Apr_Complete.RData")
#May
countsMay = data.frame(key = sapply(TPsubredMay,get_val,i=1),
                       value = sapply(TPsubredMay,get_val,i=2),
                       stringsAsFactors=FALSE)
sortMay = countsMay[with(countsMay, order(-value)), ]
sortMay$rank = seq.int(nrow(sortMay))
Top25May = head(sortMay,25)
#Compare with the words in the previous month
Top25May = merge(Top25May,sortApr,by = "key",all.x = T,all.y = F)
Top25May = Top25May[with(Top25May, order(-value.x)), ]
colnames(Top25May) = c("Subreddit","This month", "This month's rank",
                       "Last month","Last month's rank")
Top25May$`Rank change` = Top25May$`Last month's rank`-Top25May$`This month's rank`
#reorder the column sequence
Top25May = Top25May[c(1,2,4,3,5,6)]
rownames(Top25May) <- NULL
save(Top25May, file="Top25May.RData")
save(sortMay, file="May_Complete.RData")

#save the five month data together
save(list=c('Top25Jan','Top25Feb','Top25Mar','Top25Apr','Top25May'), file="Top25.RData")

#Xtable
```{r,echo=F,results='asis',comment=F}
library(xtable)
  print(xtable(Top25Jan,caption='January',digits=0),comment=FALSE)
  print(xtable(Top25Feb,caption='Feburary',digits=0),comment=FALSE)
  print(xtable(Top25Mar,caption='March',digits=0),comment=FALSE)
  print(xtable(Top25Apr,caption='April',digits=0),comment=FALSE)
  print(xtable(Top25May,caption='May',digits=0),comment=FALSE)
```

