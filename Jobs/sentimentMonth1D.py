from functions.sentimentMonth1D import *
from functions.util import *

def get1dMonthlySentiment(sentiment_pairs):
    sentiment_groupByMonth = sentiment_pairs.groupByKey()
    processed_sentiment_groupByMonth = sentiment_groupByMonth.map(
        lambda monthTuple: sentiment_process(monthTuple[0], monthTuple[1])
    )

    columName = ["Month", "Nested_List"]
    processed_sentiment_groupByMonth = processed_sentiment_groupByMonth.toDF(columName)
    df_sentiment = processed_sentiment_groupByMonth.select(
        processed_sentiment_groupByMonth.Month,
        F.explode(processed_sentiment_groupByMonth.Nested_List),
    )
    # Monthstring = udf(lambda x: "Sentiment_" + str(x[0]) + "-" + str(x[1]), T.StringType())
    Monthstring = udf(lambda x: getMonthString(x,prefix='Sentiment'), T.StringType())
    getCategory2Column = udf(lambda x: str(x[2]), T.StringType())
    df_sentiment = df_sentiment.withColumn('Category2',getCategory2Column("Month"))
    df_sentiment = df_sentiment.withColumn("Month", Monthstring("Month"))
    getTopic = udf(lambda x: x[0], T.StringType())
    getSentiments = udf(lambda x: x[1], T.FloatType())
    df_sentiment = df_sentiment.withColumn("Topic", getTopic("col"))
    df_sentiment = df_sentiment.withColumn("Sentiment", getSentiments("col"))
    cols = ['Category2',"Month", "Topic", "Sentiment"]
    df_sentiment = df_sentiment.select(*cols)
    df_sentiment = df_sentiment.groupby(["Topic",'Category2']).pivot("Month").max("Sentiment").fillna(0)
    df_sentiment = df_sentiment.withColumn('Category1',F.lit('Beverage'))

    # add missing months

    df_sentiment = fillingTheMissingMonth(df_sentiment,prefix='Sentiment')

    df_sentiment = df_sentiment.filter(df_sentiment.Topic!='empty')
    return df_sentiment