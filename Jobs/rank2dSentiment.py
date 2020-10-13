from functions.constants import *
from functions.rank2dSentiment import get2DSentimentDfFilterByTimeWindow,getSentimentGrowthOfTimeWindow2d
def getRank2dSentiment(sentiment_2d_pairs):
    df_2d_sentiment_2020LastYear = get2DSentimentDfFilterByTimeWindow(
    sentiment_pairs=sentiment_2d_pairs,
    TIMEWINDOW=LAST_Year_YEAR2020,
    TIMEWINDOWNAME="LASTYear_2020",
)
    df_2d_sentiment_2019LastYear = get2DSentimentDfFilterByTimeWindow(
        sentiment_pairs=sentiment_2d_pairs,
        TIMEWINDOW=LAST_Year_YEAR2019,
        TIMEWINDOWNAME="LASTYear_2019",
    )

    df_2d_sentiment_2020Last6Month = get2DSentimentDfFilterByTimeWindow(
        sentiment_pairs=sentiment_2d_pairs,
        TIMEWINDOW=LAST_SIX_MONTHS_YEAR2020,
        TIMEWINDOWNAME="LAST6MONTH_2020",
    )
    df_2d_sentiment_2019Last6Month = get2DSentimentDfFilterByTimeWindow(
        sentiment_pairs=sentiment_2d_pairs,
        TIMEWINDOW=LAST_SIX_MONTHS_YEAR2019,
        TIMEWINDOWNAME="LAST6MONTH_2019",
    )


    df_2d_sentiment_2020Last3Month = get2DSentimentDfFilterByTimeWindow(
        sentiment_pairs=sentiment_2d_pairs,
        TIMEWINDOW=LAST_THREE_MONTHS_YEAR2020,
        TIMEWINDOWNAME="LAST3MONTH_2020",
    )
    df_2d_sentiment_2019Last3Month = get2DSentimentDfFilterByTimeWindow(
        sentiment_pairs=sentiment_2d_pairs,
        TIMEWINDOW=LAST_THREE_MONTHS_YEAR2019,
        TIMEWINDOWNAME="LAST3MONTH_2019",
    )

    df_2d_sentiment_2020LastMonth = get2DSentimentDfFilterByTimeWindow(
        sentiment_pairs=sentiment_2d_pairs,
        TIMEWINDOW=LAST_ONE_MONTHS_YEAR2020,
        TIMEWINDOWNAME="LASTMONTH_2020",
    )
    df_2d_sentiment_2019LastMonth = get2DSentimentDfFilterByTimeWindow(
        sentiment_pairs=sentiment_2d_pairs,
        TIMEWINDOW=LAST_ONE_MONTHS_YEAR2019,
        TIMEWINDOWNAME="LASTMONTH_2019",
    )
    cols_lastyear = ["SentimentScore_LASTYear_2019", "SentimentScore_LASTYear_2020"]
    df_growth_lastyear_2d = getSentimentGrowthOfTimeWindow2d(
        df_2d_sentiment_2019LastYear,
        df_2d_sentiment_2020LastYear,
        cols_lastyear,
        TimeWindowName="last year",
    )

    cols_last6month = ["SentimentScore_LAST6MONTH_2019", "SentimentScore_LAST6MONTH_2020"]
    df_growth_last6month_2d = getSentimentGrowthOfTimeWindow2d(
        df_2d_sentiment_2019Last6Month,
        df_2d_sentiment_2020Last6Month,
        cols_last6month,
        TimeWindowName="last 6 month",
    )

    # df_growth_lastyear.show()

    cols_last3month = ["SentimentScore_LAST3MONTH_2019", "SentimentScore_LAST3MONTH_2020"]
    df_growth_last3month_2d = getSentimentGrowthOfTimeWindow2d(
        df_2d_sentiment_2019Last3Month,
        df_2d_sentiment_2020Last3Month,
        cols_last3month,
        TimeWindowName="last 3 month",
    )

    cols_last1month = ["SentimentScore_LASTMONTH_2019", "SentimentScore_LASTMONTH_2020"]
    df_growth_lastmonth_2d = getSentimentGrowthOfTimeWindow2d(
        df_2d_sentiment_2019LastMonth,
        df_2d_sentiment_2020LastMonth,
        cols_last1month,
        TimeWindowName="last month",
    )

    # df_growth_lastmonth_2d.show()
    rank2D_sent = df_growth_lastyear_2d.union(df_growth_last6month_2d)
    rank2D_sent = rank2D_sent.union(df_growth_last3month_2d)
    rank2D_sent = rank2D_sent.union(df_growth_lastmonth_2d)
    return rank2D_sent
