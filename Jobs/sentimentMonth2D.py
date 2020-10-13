from functions.sentimentMonth2D import sentiment_process_2d
from functions.util import *


def get2dMonthlySentiment(sentiment_2d_pairs):
    sentiment2dGroupByMonth = sentiment_2d_pairs.groupByKey()

    processed_sentiment2dGroupByMonth = sentiment2dGroupByMonth.map(
        lambda monthTuple: sentiment_process_2d(monthTuple[0], monthTuple[1])
    )

    # print(processed_sentiment2dGroupByMonth.collect())
    columName = ["Month", "Nested_List"]
    processed_sentiment2dGroupByMonth = processed_sentiment2dGroupByMonth.toDF(columName)

    df_sentiment_2d = processed_sentiment2dGroupByMonth.select(
        processed_sentiment2dGroupByMonth.Month,
        F.explode(processed_sentiment2dGroupByMonth.Nested_List),
    )

    Monthstring = udf(lambda x: getMonthString(x,prefix='Sentiment'), T.StringType())

    getCategory2Column = udf(lambda x: str(x[2]), T.StringType())
    df_sentiment_2d = df_sentiment_2d.withColumn('Category2',getCategory2Column("Month"))
    df_sentiment_2d = df_sentiment_2d.withColumn("Month", Monthstring("Month"))
    getTopic = udf(lambda x: x[0][0], T.StringType())
    getTopic2 = udf(lambda x: x[0][1], T.StringType())
    getTopicPair = udf(lambda x: [x[0][1], x[0][0]])
    getSentiments = udf(lambda x: x[1], T.FloatType())
    df_sentiment_2d = df_sentiment_2d.withColumn("Topic", getTopic("col"))
    df_sentiment_2d = df_sentiment_2d.withColumn("Topic2", getTopic2("col"))
    df_sentiment_2d = df_sentiment_2d.withColumn("TopicPair", getTopicPair("col"))
    df_sentiment_2d = df_sentiment_2d.withColumn("Sentiment", getSentiments("col"))
    df_sentiment_2d = (
        df_sentiment_2d.groupby('Category2',"Topic", "Topic2").pivot("Month").max("Sentiment").fillna(0)
    )
    df_sentiment_2d = df_sentiment_2d.withColumn('Category1',F.lit('Beverage'))
    # save df_sentiment_2d
    df_sentiment_2d = df_sentiment_2d.filter(df_sentiment_2d.Topic!='empty')
    df_sentiment_2d = df_sentiment_2d.filter(df_sentiment_2d.Topic2!='empty')

    df_sentiment_2d = fillingTheMissingMonth(df_sentiment_2d,prefix='Sentiment')
    return df_sentiment_2d