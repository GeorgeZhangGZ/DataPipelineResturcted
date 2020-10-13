
from functools import reduce
from operator import add, index
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf, array
from functions.constants import *
def getCV(df_freqmonth):
    df_CV = df_freqmonth
    df_CV =df_CV.groupby('Category2','Category1').agg(*[F.sum(x).alias(x) for x in df_CV.columns if x not in {'_c0',"Category2",'Topic','Category1'}])
    getSum = udf(lambda monthdata: reduce(lambda x,y:x+y,monthdata))
    getGrowthCV = udf(lambda y2020,y2019:(y2020-y2019)/(y2019+0.01))
    df_CV = df_CV.withColumn('SumFrequency_Year2020',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_Year_YEAR2020_STRING_LIST]))
    df_CV = df_CV.withColumn('SumFrequency_Year2019',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_Year_YEAR2019_STRING_LIST]))
    df_CV =df_CV.withColumn('Frequency_Yearly_Growth',getGrowthCV('SumFrequency_Year2020','SumFrequency_Year2019'))
    df_CV = df_CV.withColumn('SumFrequency_Year2020',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_SIX_MONTH_YEAR2020_STRING_LIST]))
    df_CV = df_CV.withColumn('SumFrequency_Year2019',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_SIX_MONTH_YEAR2019_STRING_LIST]))
    df_CV = df_CV.withColumn('Frequency_YOY_Growth_Last6Month',getGrowthCV('SumFrequency_Year2020','SumFrequency_Year2019'))
    df_CV = df_CV.withColumn('SumFrequency_Year2020',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_THREE_MONTH_YEAR2020_STRING_LIST]))
    df_CV = df_CV.withColumn('SumFrequency_Year2019',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_THREE_MONTH_YEAR2019_STRING_LIST]))
    df_CV = df_CV.withColumn('Frequency_YOY_Growth_Last3Month',getGrowthCV('SumFrequency_Year2020','SumFrequency_Year2019'))
    df_CV = df_CV.withColumn('SumFrequency_Year2020',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_ONE_MONTH_YEAR2020_STRING_LIST]))
    df_CV = df_CV.withColumn('SumFrequency_Year2019',reduce(add,[F.col(x) for x in df_CV.columns if x in LAST_ONE_MONTH_YEAR2019_STRING_LIST]))
    df_CV = df_CV.withColumn('Frequency_YOY_Growth_Last1Month',getGrowthCV('SumFrequency_Year2020','SumFrequency_Year2019'))
    df_CV =df_CV.drop('SumFrequency_Year2019','SumFrequency_Year2020')
    return df_CV