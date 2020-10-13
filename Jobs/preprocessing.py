from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType
from pyspark.sql.functions import rand
from functions.preprocessing import *
import numpy as np
from functions.sentiment_analysis import VaderSentiment

def getPreprocessingDataframe(df,nlp,version = 'dev'):
    if version == 'dev':
        df = df.orderBy(rand())
        df2 = df.filter(df.Timestamp.isNotNull())
        df2 = df2.limit(500)
    else:
        df = df.orderBy(rand())
        df2 = df.filter(df.Timestamp.isNotNull())

# convert timestamp to the right format
    timeStampPreCleaning = udf(
        lambda x: str(x) + " 2020" if len(x) < 8 else x.replace(",", ""), StringType()
    )
    df2 = df2.withColumn("Timestamp", timeStampPreCleaning("Timestamp"))

    # StirngToDateType
    df3 = df2.withColumn("TimeStampDateType", F.to_date(F.col("Timestamp"), "MMM dd yyyy"))

    # drop null value rows which timestamp columns is not in the standard format.
    df3 = df3.filter(df3.Text.isNotNull())
    df3 = df3.filter(df3.TimeStampDateType.isNotNull())
    df3 = df3.withColumn("Year", F.year(df3.TimeStampDateType))
    df3 = df3.withColumn("Month", F.month(df3.TimeStampDateType))
    df3 = df3.withColumn("Qurter", F.quarter(df3.TimeStampDateType))
    # fill null with 0 and convert unit to the right numbers.
    cols = ["Comments", "Likes", "Retweets"]

    df3 = df3.fillna("0", subset=cols)

    # apply the transform_number udf
    transformNumber = udf(lambda z: transform_number(z), T.IntegerType())
    df3 = df3.withColumn("Comments", transformNumber("Comments"))
    df3 = df3.withColumn("Likes", transformNumber("Likes"))
    df3 = df3.withColumn("Retweets", transformNumber("Retweets"))
    ### check
    logNormal = udf(lambda x: int(round(np.log2(x + 1)))+1, T.IntegerType())
    df3 = df3.withColumn("Likes_log", logNormal("Likes"))
    df3 = df3.withColumn("Retweets_log", logNormal("Retweets"))
    # df3 = df3.filter(df3.Likes_log.isNotNull())
    df3 = df3.filter(df3.Retweets_log.isNotNull())
    
    extractKeywordFromQueries = udf(lambda x: extractkeyword(x))
    df3 = df3.filter(df3.Page_URL.isNotNull())
    df3 = df3.withColumn("Keyword", extractKeywordFromQueries("Page_URL"))
    df3 = df3.filter(df3.Keyword.isNotNull())

    
    keywordToCategory2 = udf(lambda x: getCategory2(x), StringType())
    df3 = df3.withColumn("Category2", keywordToCategory2("Keyword"))

    # NER Model
    # could be empty list,
    nerExtraction = udf(lambda z: ner_extraction(z,nlp), T.ArrayType(StringType()))

    df3 = df3.withColumn("All_phrases", nerExtraction("Text"))
    df3 = df3.filter(df3.All_phrases.isNotNull())

    checkEmpty = udf(lambda x: checkempty(x), T.IntegerType())

    df3 = df3.withColumn('CheckEmpty',checkEmpty('All_phrases'))
    df3 = df3.filter(df3.CheckEmpty.isNotNull())


    df3 = df3.filter(df3.CheckEmpty != int(1))

    sentiment = VaderSentiment()
    vader_sentiment = udf(sentiment.score,T.FloatType())
    df3 = df3.withColumn("Sentiment",vader_sentiment('Text'))
    
    weighted_phrases_calculate = udf(
    lambda x, y: y * (int(x) + 1), T.ArrayType(StringType())
)


    df3 = df3.withColumn(
        "Weighted_phrases", weighted_phrases_calculate("Retweets_log", "All_phrases")
    )



    # cols = ['Sentiment','All_phrases','Retweets_log','Weighted_phrases','Year','Month','Keyword']
    cols = ["Weighted_phrases", "Year", "Month", "Keyword", "Category2"]


    weighted_phrases_calculate = udf(lambda x, y: y * (int(x) + 1), T.FloatType())

    # get the weighted sentiments for each tweets.
    df3 = df3.withColumn(
        "Weighted_Sentiment", weighted_phrases_calculate("Likes_log", "Sentiment")
    )
    return df3