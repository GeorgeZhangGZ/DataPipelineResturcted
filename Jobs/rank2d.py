from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf


def getRank2d(df_pattern, rank2D_sent, rank2D_freq, df_noveltyScore2d):
    rank2d = rank2D_sent.join(
        rank2D_freq, ["Category2", "Topic", "Topic2", "Date"], how="outer"
    )

    # rank2d.toPandas().to_csv('RANKD2D_new.csv')
    # rank2d.show()
    df_pattern = df_pattern.select(["id", "label"])
    df_pattern = df_pattern.withColumnRenamed("label", "Perspective")
    df_pattern = df_pattern.withColumnRenamed("id", "Topic")
    rank2d = rank2d.join(df_pattern, ["Topic"], how="left")
    df_pattern = df_pattern.withColumnRenamed("Topic", "Topic2")
    df_pattern = df_pattern.withColumnRenamed("Perspective", "Perspective2")
    rank2d = rank2d.join(df_pattern, ["Topic2"], how="left")
    rank2d = rank2d.fillna("Hashtag", subset=["Perspective"])
    rank2d = rank2d.fillna("Hashtag", subset=["Perspective2"])
    # getTrendingScore =udf(lambda x,y:int(x)*0.8+int(y)*0.2)
    # rank2d = rank2d.withColumn('Trending_Score',getTrendingScore('Frequency_YoY_Growth','Sentiment_YoY_Growth'))
    rank2d = rank2d.withColumn("Category1", F.lit("Beverage"))
    rank2d = rank2d.withColumn("Country", F.lit("Canada"))

    # rank2d = rank2d.withColumn('Novelty_Score',F.lit(5))

    rank2d = rank2d.join(
        df_noveltyScore2d, ["Topic", "Topic2", "Category2"], how="left"
    )
    rank2d = rank2d.sort(F.col("Trending_Score").desc())
    rank2d = rank2d.drop("_c0")
    rank2d = rank2d.filter(rank2d.Topic != "empty")
    rank2d = rank2d.filter(rank2d.Topic2 != "empty")
    return rank2d