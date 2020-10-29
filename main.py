from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType
from textblob import TextBlob
import nltk
from pyspark.sql.functions import rand
import math
import pickle
# import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import datetime
import numpy as np
import re
from operator import add, index
from functools import reduce
from itertools import combinations
# load spacy and ner model
import spacy
import os
import sys

from Jobs.preprocessing import *
from Jobs.freqmonth1d import *
from functions.util import *
from Jobs.rank1dfreq import *
from Jobs.sentimentMonth1D import *
from Jobs.rank1dSentiment import *
from Jobs.rank1d import *
from Jobs.sentimentMonth2D import *
from Jobs.rank2dSentiment import *
from Jobs.freqmonth2d import *
from Jobs.rank2dfreq import *
from Jobs.rank2d import *
from Jobs.CV import *




if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')
    

nlp = spacy.load("NER_model")

spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# get all the df in the df directory.
# df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("./data2/*.csv")
df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("s3://tasteguru-trend-project-test/data2/*.csv")


df3 = getPreprocessingDataframe(df,nlp,version='dev')
pairs = df3.rdd.map(
    lambda row: ((row.Year, row.Month, row.Category2), row.Weighted_phrases)
)
df_freqmonth = get1dMonthlyFreq(pairs)
df_freqmonth.show()


df_lastYearTrendingScore = getTrendingScoreOfTimeWindow(df_freqmonth=df_freqmonth,startDate='Frequency_2019-07',endDate='Frequency_2020-06')
df_last1monthTrendingScore = getTrendingScoreOfTimeWindow(df_freqmonth=df_freqmonth,startDate='Frequency_2020-04',endDate='Frequency_2020-06')
df_last3monthTrendingScore = getTrendingScoreOfTimeWindow(df_freqmonth=df_freqmonth,startDate='Frequency_2020-03',endDate='Frequency_2020-06')
df_last6monthTrendingScore = getTrendingScoreOfTimeWindow(df_freqmonth=df_freqmonth,startDate='Frequency_2020-01',endDate='Frequency_2020-06')

### get novelty score
frequency_col = df_freqmonth.columns
target = datetime.strptime('2020-06', '%Y-%m') # Target date (can be modified but HAVE TO BE IN THE SAME FORMAT)
timeframe = TimeFrame(target_date = target, columns = frequency_col)
first_day_col, last_day_col = timeframe.date_col(string = 'Frequency_')
max_interval = timeframe.get_target_days()
df_freqmonth_novel = df_freqmonth.withColumn("Max_interval", F.lit(max_interval))
first_appearance = udf(timeframe.get_first_appearance,T.IntegerType())
col = [x for x in df_freqmonth.columns]
df_freqmonth_novel = df_freqmonth_novel.withColumn("First_appearance",first_appearance(*col))
novelty_score = udf(novelty, T.FloatType())
df_freqmonth_novel = df_freqmonth_novel.withColumn('Novelty_Score',novelty_score('Max_interval','First_appearance'))
                                       
novelty_cols = ['Topic','Novelty_Score','Category2']
df_noveltyScore = df_freqmonth_novel.select(*novelty_cols)
df_noveltyScore.toPandas().to_csv('df_novelty.csv',index=False)

# df_freqmonth = fillingTheMissingMonth(df_freqmonth,prefix='Frequency')
# rank1D_freq = getRank1dFreq(pairs,df_lastYearTrendingScore,df_last1monthTrendingScore,df_last3monthTrendingScore,df_last6monthTrendingScore)
# # df_freqmonth.toPandas().to_csv('s3n://tasteguru-trend-project-test/modelresult/Frequency_new_demo.csv',index=False)
# # df_freqmonth.show()

# sentiment_pairs = df3.rdd.map(
#     lambda row: (
#         (row.Year, row.Month,row.Category2),
#         (row.All_phrases, row.Weighted_Sentiment, row.Likes_log),
#     )
# )

# df_sentiment = get1dMonthlySentiment(sentiment_pairs)
# df_sentiment.toPandas().to_csv('Sentiments_new.csv',index=False)


# ## get rank1dsentiment
# rank1D_sent = getRank1dSentiment(sentiment_pairs)

# df_pattern = spark.read.json('s3://tasteguru-trend-project-test/patterns.jsonl')
# # df_pattern = spark.read.json('patterns.jsonl')

# rank1d = getRank1d(df_pattern,rank1D_sent,rank1D_freq,df_noveltyScore)
# rank1d.toPandas().to_csv("RANKD1D_new.csv",index=False)

# sentiment_2d_pairs = df3.rdd.map(
#     lambda row: (
#         (row.Year, row.Month,row.Category2),
#         (
#             iterToList(combinations(row.All_phrases, 2)),
#             row.Weighted_Sentiment,
#             row.Likes_log,
#         ),
#     )
# )

# df_sentiment_2d = get2dMonthlySentiment(sentiment_2d_pairs)
# df_sentiment_2d.toPandas().to_csv('Sentiment2D_new.csv',index=False)

# rank2D_sent = getRank2dSentiment(sentiment_2d_pairs)

# freq_2d_pairs = df3.rdd.map(
#     lambda row: (
#         (row.Year, row.Month,row.Category2),
#         (iterToList(combinations(row.All_phrases, 2)), row.Retweets_log),
#     )
# )
# df_freq_month = get2dMonthlyFreq(freq_2d_pairs)

# df_lastYearTrendingScore2d = getTrendingScoreOfTimeWindow2D(df_freqmonth=df_freq_month,startDate='Frequency_2019-07',endDate='Frequency_2020-06')
# df_last1monthTrendingScore2d = getTrendingScoreOfTimeWindow2D(df_freqmonth=df_freq_month,startDate='Frequency_2020-04',endDate='Frequency_2020-06')
# df_last3monthTrendingScore2d = getTrendingScoreOfTimeWindow2D(df_freqmonth=df_freq_month,startDate='Frequency_2020-03',endDate='Frequency_2020-06')
# df_last6monthTrendingScore2d = getTrendingScoreOfTimeWindow2D(df_freqmonth=df_freq_month,startDate='Frequency_2020-01',endDate='Frequency_2020-06')




# frequency_col = df_freq_month.columns
# target = datetime.strptime('2020-06', '%Y-%m') # Target date (can be modified but HAVE TO BE IN THE SAME FORMAT)
# timeframe = TimeFrame(target_date = target, columns = frequency_col)
# first_day_col, last_day_col = timeframe.date_col(string = 'Frequency_')
# max_interval = timeframe.get_target_days()
# df_noval2d = df_freq_month.withColumn("Max_interval", F.lit(max_interval))
# first_appearance = udf(timeframe.get_first_appearance,T.IntegerType())
# col = [x for x in df_noval2d.columns]
# df_noval2d = df_noval2d.withColumn("First_appearance",first_appearance(*col))
# novelty_score = udf(novelty, T.FloatType())
# df_noval2d = df_noval2d.withColumn('Novelty_Score',novelty_score('Max_interval','First_appearance'))
                                       
# novelty_cols = ['Topic','Topic2','Novelty_Score','Category2']
# df_noveltyScore2d = df_noval2d.select(*novelty_cols)



# df_freq_month = fillingTheMissingMonth(df_freq_month,prefix='Frequency')

# df_freq_month.toPandas().to_csv('Frequency_2d_new.csv',index=False)



# rank2D_freq = getRank2dFreq(freq_2d_pairs,df_lastYearTrendingScore2d,df_last6monthTrendingScore2d,df_last3monthTrendingScore2d,df_last1monthTrendingScore2d)
# rank2d = getRank2d(df_pattern, rank2D_sent, rank2D_freq, df_noveltyScore2d)
# rank2d.toPandas().to_csv("RANKD2D_new.csv",index = False)

# df_CV = getCV(df_freqmonth=df_freqmonth)
# df_CV.toPandas().to_csv('TG_CV.csv',index=False)
