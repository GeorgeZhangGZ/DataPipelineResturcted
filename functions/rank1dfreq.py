
from functions.util import merge
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf

def getFreqDfByTimeWindow(rdd, TIMEWINDOW, TIMEWINDOWNAME):
    # filterByTimeWindow = rdd.filter(lambda x: x[0] in TIMEWINDOW)

    filterByTimeWindow = rdd.filter(lambda x: (x[0][0], x[0][1]) in TIMEWINDOW)
    filterByTimeWindow = filterByTimeWindow.map(lambda x: (x[0][2], x[1]))
    filterByTimeWindow = filterByTimeWindow.groupByKey()
    filterByTimeWindow = filterByTimeWindow.map(lambda x: merge(x[0], x[1]))
    columName = ["Month", "Nested_List"]
    df_freq = filterByTimeWindow.toDF(columName)

    df_freq = df_freq.select(df_freq.Month, F.explode(df_freq.Nested_List))
    getKeyword = udf(lambda z: z[0], T.StringType())
    getFrequency = udf(lambda z: z[1], T.IntegerType())
    df_freq = df_freq.withColumn("Topic", getKeyword("col"))
    df_freq = df_freq.withColumn(f"Frequency_{TIMEWINDOWNAME}", getFrequency("col"))
    columns = ["Month", "Topic", f"Frequency_{TIMEWINDOWNAME}"]
    df_freq = df_freq.select(*columns)
    # df_freq = df_freq.select(*cols)
    # df_freqmonth.show()

    return df_freq

def getFrequencyGrowthOfTimeWindow(dfYear19, dfYear20, cols, TimeWindowName):
    df = dfYear20.join(dfYear19, on=["Topic", "Month"], how="outer")
    df = df.fillna(0, subset=cols)
    #
    getGrowth = udf(lambda y19, y20: (y20 - y19) / (y19 + 1), T.FloatType())
    df = df.withColumn("Frequency_YoY_Growth", getGrowth(cols[0], cols[1]))
    df = df.withColumn("Date", F.lit(f"{TimeWindowName}"))
    df = df.withColumnRenamed(cols[1], "Frequency")
    # need to filter the frequency == 0 rows
    # df= df.filter(df.Frequency != 0.0)
    df = df.withColumnRenamed("Month", "Category2")
    cols = ["Category2", "Topic", "Frequency", "Frequency_YoY_Growth", "Date"]
    return df.select(*cols)