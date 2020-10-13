from pyspark.sql.functions import udf
from pyspark.sql import functions as F, types as T
from functions.sentimentMonth2D import sentiment_process_2d
def get2DSentimentDfFilterByTimeWindow(sentiment_pairs, TIMEWINDOW, TIMEWINDOWNAME):
    columName = ["Month", "Nested_List"]
    # filter rdd by timewindow
    filterByTimeWindow = sentiment_pairs.filter(lambda x: (x[0][0], x[0][1]) in TIMEWINDOW)
    # create new key
    filterByTimeWindow = filterByTimeWindow.map(lambda x: (x[0][2], x[1]))
    filterByTimeWindow = filterByTimeWindow.groupByKey()
    filterByTimeWindow = filterByTimeWindow.map(
        lambda monthTuple: sentiment_process_2d(monthTuple[0], monthTuple[1])
    )
    df_filterByTimeWindow = filterByTimeWindow.toDF(columName)
    # explode nested list
    df_filterByTimeWindow = df_filterByTimeWindow.select(
        df_filterByTimeWindow.Month, F.explode(df_filterByTimeWindow.Nested_List)
    )
    # df_filterByTimeWindow.show()
    getTopic = udf(lambda x: x[0][0], T.StringType())
    getTopic2 = udf(lambda x: x[0][1], T.StringType())
    getTopicPair = udf(lambda x: [x[0][1], x[0][0]])
    getSentiments = udf(lambda x: x[1], T.FloatType())

    # getKeyword = udf(lambda z: z[0], T.StringType())
    # getSentimentScore = udf(lambda z: z[1], T.FloatType())
    # df_filterByTimeWindow = df_filterByTimeWindow.withColumn(
    #     "Topic", getKeyword("col")
    # )
    # df_filterByTimeWindow = df_filterByTimeWindow.withColumn(
    #     f"SentimentScore_{TIMEWINDOWNAME}", getSentimentScore("col")
    # )
    # df_filterByLastSixMonth2020.show()

    df_filterByTimeWindow = df_filterByTimeWindow.withColumn("Topic", getTopic("col"))
    df_filterByTimeWindow = df_filterByTimeWindow.withColumn("Topic2", getTopic2("col"))
    # df_filterByTimeWindow = df_filterByTimeWindow.withColumn("TopicPair",getTopicPair("col"))
    df_filterByTimeWindow = df_filterByTimeWindow.withColumn(
        f"SentimentScore_{TIMEWINDOWNAME}", getSentiments("col")
    )

    columns = ['Month',"Topic", "Topic2", f"SentimentScore_{TIMEWINDOWNAME}"]
    df_filterByTimeWindow = df_filterByTimeWindow.select(*columns)
    return df_filterByTimeWindow


def getSentimentGrowthOfTimeWindow2d(dfYear19, dfYear20, cols, TimeWindowName):
    df = dfYear20.join(dfYear19, on=["Topic", "Topic2",'Month'], how="outer")
    df = df.fillna(0, subset=cols)
    getGrowth = udf(lambda y19, y20: (y20 - y19) / (y19 + 0.01), T.FloatType())
    df = df.withColumn("Sentiment_YoY_Growth", getGrowth(cols[0], cols[1]))
    df = df.withColumn("Date", F.lit(f"{TimeWindowName}"))
    df = df.withColumnRenamed(cols[1], "Sentiment")
    df = df.withColumnRenamed("Month", "Category2")
    cols = ['Category2',"Topic", "Topic2", "Sentiment", "Sentiment_YoY_Growth", "Date"]
    return df.select(*cols)