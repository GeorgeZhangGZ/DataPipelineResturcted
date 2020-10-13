from functions.freqmonth2d import combinationWithRetweets
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf, array
def getFreqDf2DByTimeWindow(rdd,TIMEWINDOW, TIMEWINDOWNAME):
    filterByTimeWindow = rdd.filter(lambda x: (x[0][0], x[0][1])  in TIMEWINDOW)
    filterByTimeWindow = filterByTimeWindow.map(lambda x: (x[0][2], x[1]))
    filterByTimeWindow = filterByTimeWindow.groupByKey()
    filterByTimeWindow = filterByTimeWindow.map(lambda monthTuple: combinationWithRetweets(monthTuple[0],monthTuple[1]))
    columName = ["Month", "Nested_List"]
    df_freq = filterByTimeWindow.toDF(columName)

    # Monthstring = udf(lambda x: "Frequency_" + str(x[0]) + "-" + str(x[1]), T.StringType())
    df_freq = df_freq.select(
    df_freq.Month, F.explode(df_freq.Nested_List)
)

    
    getTopic = udf(lambda x: x[0][0], T.StringType())
    getTopic2 = udf(lambda x: x[0][1], T.StringType())
    getTopicPair = udf(lambda x: [x[0][1], x[0][0]])
    getFrequency = udf(lambda x: x[1], T.IntegerType())
    df_freq = df_freq.withColumn("Topic", getTopic("col"))
    df_freq = df_freq.withColumn("Topic2", getTopic2("col"))
    df_freq = df_freq.withColumn("TopicPair", getTopicPair("col"))
    df_freq = df_freq.withColumn(f"Frequency_{TIMEWINDOWNAME}", getFrequency("col"))
    # df_filterByLastSixMonth2020.show()
    columns = ['Month',"Topic", 'Topic2',f"Frequency_{TIMEWINDOWNAME}"]
    df_freq = df_freq.select(*columns)

    return df_freq


def getFrequencyGrowthOfTimeWindow2d(dfYear19, dfYear20, cols, TimeWindowName):
    df = dfYear20.join(dfYear19, on=["Topic", "Topic2",'Month'], how="outer")
    df = df.fillna(0, subset=cols)
    getGrowth = udf(lambda y19, y20: (y20 - y19) / (y19 + 0.01), T.FloatType())
    df = df.withColumn("Frequency_YoY_Growth", getGrowth(cols[0], cols[1]))
    df = df.withColumn("Date", F.lit(f"{TimeWindowName}"))
    df = df.withColumnRenamed(cols[1], "Frequency")
    # df = df.filter(df.Frequency != 0.0)
    df = df.withColumnRenamed("Month", "Category2")
    cols = ['Category2',"Topic", "Topic2", "Frequency", "Frequency_YoY_Growth", "Date"]
    return df.select(*cols)