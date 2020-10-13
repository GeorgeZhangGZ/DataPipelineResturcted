from functions.sentimentMonth1D import sentiment_process
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf

def getDfFilterByTimeWindow(sentiment_pairs, TIMEWINDOW, TIMEWINDOWNAME):
    columName = ["Month", "Nested_List"]
    filterByTimeWindow = sentiment_pairs.filter(lambda x: (x[0][0], x[0][1])  in TIMEWINDOW)
    filterByTimeWindow = filterByTimeWindow.map(lambda x: (x[0][2], x[1]))
    filterByTimeWindow = filterByTimeWindow.groupByKey()
    filterByTimeWindow = filterByTimeWindow.map(
        lambda monthTuple: sentiment_process(monthTuple[0], monthTuple[1])
    )
    df_filterByTimeWindow = filterByTimeWindow.toDF(columName)
    # df_filterByTimeWindow.show()
    df_filterByTimeWindow = df_filterByTimeWindow.select(
        df_filterByTimeWindow.Month, F.explode(df_filterByTimeWindow.Nested_List)
    )
    # df_filterByTimeWindow.show()
    getKeyword = udf(lambda z: z[0], T.StringType())
    getSentimentScore = udf(lambda z: z[1], T.FloatType())
    df_filterByTimeWindow = df_filterByTimeWindow.withColumn("Topic", getKeyword("col"))
    df_filterByTimeWindow = df_filterByTimeWindow.withColumn(
        f"SentimentScore_{TIMEWINDOWNAME}", getSentimentScore("col")
    )
    # df_filterByLastSixMonth2020.show()
    columns = ["Month","Topic", f"SentimentScore_{TIMEWINDOWNAME}"]
    df_filterByTimeWindow = df_filterByTimeWindow.select(*columns)
    return df_filterByTimeWindow

def getSentimentGrowthOfTimeWindow(dfYear19, dfYear20, cols, TimeWindowName):
    df = dfYear20.join(dfYear19, on=["Topic","Month"], how="outer")
    df = df.fillna(0, subset=cols)
    getGrowth = udf(lambda y19, y20: (y20 - y19) / (y19 + 0.01), T.FloatType())
    df = df.withColumn("Sentiment_YoY_Growth", getGrowth(cols[0], cols[1]))
    df = df.withColumn("Date", F.lit(f"{TimeWindowName}"))
    df = df.withColumnRenamed(cols[1], "Sentiment")
    df = df.withColumnRenamed("Month", "Category2")
    cols = ["Category2","Topic", "Sentiment", "Sentiment_YoY_Growth", "Date"]
    return df.select(*cols)