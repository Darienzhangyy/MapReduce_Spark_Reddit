#Background

Over the last year Reddit user Stuck_In_the_Matrix has been actively scraping Reddit making the data publicly available for researchers and other interested parties. The data is collected at the comment level, each entry contains a user’s comment along with relevant metadata (subreddit, date time, score, etc). This data is stored as text multiple JSON files, where each line is a separate JSON object. Due to the high volume of traffic on Reddit each of the monthly files is approximately 30 gigabytes. You will be responsible performing the following analysis tasks using Hadoop and/or Spark.


#1 - What’s popular with Redditors?
Each comment belongs to a particular subreddit, we would like to know for the given time period what were the 25 most popular subreddits? We would also like to have this broken down by month (Jan to May) - create a Billboard-like table for each month that shows the top 25 subreddits for that month that also includes the change in rank since the previous month. Comment on any subreddits that show a strong positive or negative trend.


#2 - When do Redditors post?
Create plots that show the frequency of Reddit comments over the entire time period (aggregate to an hourly level). Also create plots that show the frequency of comments over the days of the week (data should again be at an hour level). Comment on any patterns you notice, particularly days with unusually large or small numbers of comments.

Recreate the above plots but only for comments that were gilded (commentor was given Reddit gold by another user). Comment on the similarly or difference in the plot collections.

#3 - What are Redditors saying (on Valentine’s Day)?
Valentine’s day is on February 14 every year, our data contains this for 2015 - pick two other dates and perform a word frequency analysis of these three days and see if what Redditors say on Valentine’s day appears to be different from your control days. This does not need to be a fully quantitative analysis but do make sure to clean up the data (e.g. strip things like punctuation and capitalization, remove stop words, etc.)


