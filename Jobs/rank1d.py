
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf
def getRank1d(df_pattern,rank1D_sent,rank1D_freq,df_noveltyScore):
    rank1d = rank1D_sent.join(rank1D_freq, ["Category2","Topic", "Date"], how="outer")



    # df_pattern = spark.read.json('./patterns.jsonl')
    df_pattern = df_pattern.select(['id','label'])
    df_pattern = df_pattern.withColumnRenamed('label','Perspective')
    df_pattern = df_pattern.withColumnRenamed('id','Topic')
    # rank1d = spark.read.options(header="True", inferSchema="True", delimiter=",").csv(
    #     "RANKD1D_new.csv"
    # )
    rank1d = rank1d.join(df_pattern,['Topic'],how='left')
    rank1d = rank1d.fillna('Hashtag',subset = ['Perspective'])
    getTrendingScore =udf(lambda x,y:int(x)*0.8+int(y)*0.2)
    # rank1d = rank1d.withColumn('Trending_Score',getTrendingScore('Frequency_YoY_Growth','Sentiment_YoY_Growth'))
    rank1d = rank1d.withColumn('Category1',F.lit('Beverage'))
    rank1d = rank1d.withColumn('Country',F.lit('Canada'))

    # ## new novality score is based on the frequecy monthly need to join the novelty score with the rank1d.
    # rank1d = rank1d.withColumn('Novelty_Score',F.lit(5))
    rank1d = rank1d.join(df_noveltyScore,['Topic','Category2'],how = 'left')
    rank1d=rank1d.drop('_c0')
    rank1d=rank1d.sort(F.col("Trending_Score").desc())
    rank1d = rank1d.filter(rank1d.Topic!='empty')
    return rank1d