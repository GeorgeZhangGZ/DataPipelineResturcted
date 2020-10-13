import nltk
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf, array
from functions.trendscore import CTM
from functions.sentiment_analysis import VaderSentiment
from functions.timereference import TimeFrame
import math

def merge(month, phrasesList):
    mergedPhraseList = []
    for phrases in phrasesList:
        mergedPhraseList.extend(phrases)
    return (month, nltk.FreqDist(mergedPhraseList).most_common())

def getMonthString(x,prefix):
    if x[1]<10:
        return f"{prefix}_" + str(x[0]) + "-0" + str(x[1])
    else:
        return f"{prefix}_" + str(x[0]) + "-" + str(x[1])
    
def getTrendingScoreOfTimeWindow(df_freqmonth,startDate,endDate):
    trendscore = CTM()
    trendscore.column_index(df_freqmonth, startDate, endDate)  # The range is specified by the users
    ctm = udf(trendscore.get_ctm,T.FloatType())
    col = [x for x in df_freqmonth.columns]
    df_freqmonth = df_freqmonth.withColumn("Trending_Score",ctm(*col))
    col = ['Category2','Topic','Trending_Score',]
    df = df_freqmonth.select(*col)
    return df

def novelty(days_til_target,days_from_first):
    if days_til_target - days_from_first > 0:
        noveltyscore = math.log10(days_til_target - days_from_first)*3
    else:
        noveltyscore = 0
    return noveltyscore


def fillingTheMissingMonth(df,prefix):
    if f'{prefix}_2018-06' not in df.columns:
        df =df.withColumn(f'{prefix}_2018-06',df[f'{prefix}_2018-02'])

    if f'{prefix}_2018-08' not in df.columns:
        df =df.withColumn(f'{prefix}_2018-08',df[f'{prefix}_2018-01'])
    
    if f'{prefix}_2019-03' not in df.columns:
        df =df.withColumn(f'{prefix}_2019-03,',df[f'{prefix}_2018-03'])
    

    if f'{prefix}_2019-06' not in df.columns:
        df = df.withColumn(f'{prefix}_2019-06',df[f'{prefix}_2018-06'])

    if f'{prefix}_2019-08' not in df.columns:
        df = df.withColumn(f'{prefix}_2019-08',df[f'{prefix}_2018-01'])

    
    if f'{prefix}_2019-11' not in df.columns:
        df = df.withColumn(f'{prefix}_2019-11',df[f'{prefix}_2018-11'])

    if f'{prefix}_2020-02' not in df.columns:
        df = df.withColumn(f'{prefix}_2020-02',df[f'{prefix}_2018-02'])


    if f'{prefix}_2020-04' not in df.columns:
        df = df.withColumn(f'{prefix}_2020-04',df[f'{prefix}_2019-04'])
        
    if f'{prefix}_2020-07' not in df.columns:
        df = df.withColumn(f'{prefix}_2020-07',df[f'{prefix}_2019-07'])
        
    if f'{prefix}_2020-08' not in df.columns:
        df = df.withColumn(f'{prefix}_2020-08',df[f'{prefix}_2018-01'])
    
    if f'{prefix}_2020-09' not in df.columns:
        df = df.withColumn(f'{prefix}_2020-09',df[f'{prefix}_2019-09'])
    return df


def iterToList(iter):
    result = []
    for pair in iter:
        result.append(pair)
    return result


def getTrendingScoreOfTimeWindow2D(df_freqmonth,startDate,endDate):
    trendscore = CTM()
    trendscore.column_index(df_freqmonth, startDate, endDate)  # The range is specified by the users
    ctm = udf(trendscore.get_ctm,T.FloatType())
    col = [x for x in df_freqmonth.columns]
    df_freqmonth = df_freqmonth.withColumn("Trending_Score",ctm(*col))
    col = ['Category2','Topic','Topic2','Trending_Score',]
    df = df_freqmonth.select(*col)
    return df

def monthColumnGenerator(prefix, MonthTupleList):
    return [prefix + "_" + str(x[0]) + "-0" + str(x[1]) if x[1]<10 else prefix + "_" + str(x[0]) + "-" + str(x[1]) for x in MonthTupleList]
