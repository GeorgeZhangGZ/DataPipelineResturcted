from functions.freqmonth2d import *
from functions.util import *

def get2dMonthlyFreq(freq_2d_pairs):
    freq_2d_pairs_grouped = freq_2d_pairs.groupByKey()
    freq_2d_pairs_processed = freq_2d_pairs_grouped.map(lambda monthTuple: combinationWithRetweets(monthTuple[0],monthTuple[1]))
        # .flatMap(lambda x: x)
        # .reduceByKey(lambda x, y: x + y)
        # .collect()
    columName = ["Month", "Nested_List"]
    processed_freq2dGroupByMonth = freq_2d_pairs_processed.toDF(columName)

    df_freq_month = processed_freq2dGroupByMonth.select(
        processed_freq2dGroupByMonth.Month,
        F.explode(processed_freq2dGroupByMonth.Nested_List),
    )
    
    Monthstring = udf(lambda x: getMonthString(x,prefix='Frequency'), T.StringType())
    # Monthstring = udf(lambda x: "Frequency_" + str(x[0]) + "-" + str(x[1]), T.StringType())
    getCategory2Column = udf(lambda x: str(x[2]), T.StringType())

    df_freq_month = df_freq_month.withColumn('Category2',getCategory2Column("Month"))
    df_freq_month = df_freq_month.withColumn("Month", Monthstring("Month"))

    getTopic = udf(lambda x: x[0][0], T.StringType())
    getTopic2 = udf(lambda x: x[0][1], T.StringType())
    getTopicPair = udf(lambda x: [x[0][1], x[0][0]])
    getFrequency = udf(lambda x: x[1], T.IntegerType())
    df_freq_month = df_freq_month.withColumn("Topic", getTopic("col"))
    df_freq_month = df_freq_month.withColumn("Topic2", getTopic2("col"))
    df_freq_month = df_freq_month.withColumn("TopicPair", getTopicPair("col"))
    df_freq_month = df_freq_month.withColumn("Frequency", getFrequency("col"))
    df_freq_month = (
        df_freq_month.groupby("Topic", "Topic2","Category2").pivot("Month").max("Frequency").fillna(0)
    )

    df_freq_month = df_freq_month.withColumn('Category1',F.lit('Beverage'))
    # print(freq_2d_pairs.collect())
    # df_freq_month.show()
    df_freq_month = df_freq_month.filter(df_freq_month.Topic!='empty')
    df_freq_month = df_freq_month.filter(df_freq_month.Topic2!='empty')
    return df_freq_month