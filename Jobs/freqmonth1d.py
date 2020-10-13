from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType
from functions.util import *
def get1dMonthlyFreq(pairs):
    # pairs = df3.rdd.map(
    #     lambda row: ((row.Year, row.Month, row.Category2), row.Weighted_phrases)
    # )
    # print(pairs.collect())

    groupByMonth = pairs.groupByKey()
    Frequency_mergeByMonth = groupByMonth.map(lambda x: merge(x[0], x[1]))
    # print(Frequency_mergeByMonth.collect())
    df_freqmonth = Frequency_mergeByMonth.toDF(["Month", "Nested_List"])
    # df_freqmonth.show()
    df_freqmonth = df_freqmonth.select(
        df_freqmonth.Month, F.explode(df_freqmonth.Nested_List)
    )
    Monthstring = udf(lambda x: getMonthString(x,prefix='Frequency'), T.StringType())
    getCategory2Column = udf(lambda x: str(x[2]), T.StringType())
    df_freqmonth = df_freqmonth.withColumn('Category2',getCategory2Column("Month"))
    df_freqmonth = df_freqmonth.withColumn("Month", Monthstring("Month"))

    getTopic = udf(lambda x: x[0], T.StringType())
    getFreq = udf(lambda x: x[1], T.IntegerType())
    df_freqmonth = df_freqmonth.withColumn("Topic", getTopic("col"))
    df_freqmonth = df_freqmonth.withColumn("Frequency", getFreq("col"))
    cols = ["Category2","Month", "Topic", "Frequency"]
    df_freqmonth = df_freqmonth.select(*cols)
    # # df_freqmonth.show()
    df_freqmonth = df_freqmonth.groupby(["Topic","Category2"]).pivot("Month").max("Frequency").fillna(0)
    df_freqmonth = df_freqmonth.withColumn('Category1',F.lit('Beverage'))
    df_freqmonth = df_freqmonth.filter(df_freqmonth.Topic!='empty')
    return df_freqmonth