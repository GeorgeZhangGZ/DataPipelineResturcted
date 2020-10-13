from functions.rank1dSentiment import *
from functions.constants import *

def getRank1dSentiment(sentiment_pairs):
    df_sentiment_2020Last6Month = getDfFilterByTimeWindow(
    sentiment_pairs=sentiment_pairs,
    TIMEWINDOW=LAST_SIX_MONTHS_YEAR2020,
    TIMEWINDOWNAME="LAST6MONTH_2020",
)
    df_sentiment_2019Last6Month = getDfFilterByTimeWindow(
        sentiment_pairs=sentiment_pairs,
        TIMEWINDOW=LAST_SIX_MONTHS_YEAR2019,
        TIMEWINDOWNAME="LAST6MONTH_2019",
    )

    df_sentiment_2020LastYear = getDfFilterByTimeWindow(
        sentiment_pairs=sentiment_pairs,
        TIMEWINDOW=LAST_Year_YEAR2020,
        TIMEWINDOWNAME="LASTYear_2020",
    )
    df_sentiment_2019LastYear = getDfFilterByTimeWindow(
        sentiment_pairs=sentiment_pairs,
        TIMEWINDOW=LAST_Year_YEAR2019,
        TIMEWINDOWNAME="LASTYear_2019",
    )



    df_sentiment_2020Last3Month = getDfFilterByTimeWindow(
        sentiment_pairs=sentiment_pairs,
        TIMEWINDOW=LAST_THREE_MONTHS_YEAR2020,
        TIMEWINDOWNAME="LAST3MONTH_2020",
    )
    df_sentiment_2019Last3Month = getDfFilterByTimeWindow(
        sentiment_pairs=sentiment_pairs,
        TIMEWINDOW=LAST_THREE_MONTHS_YEAR2019,
        TIMEWINDOWNAME="LAST3MONTH_2019",
    )

    df_sentiment_2020LastMonth = getDfFilterByTimeWindow(
        sentiment_pairs=sentiment_pairs,
        TIMEWINDOW=LAST_ONE_MONTHS_YEAR2020,
        TIMEWINDOWNAME="LASTMONTH_2020",
    )
    df_sentiment_2019LastMonth = getDfFilterByTimeWindow(
        sentiment_pairs=sentiment_pairs,
        TIMEWINDOW=LAST_ONE_MONTHS_YEAR2019,
        TIMEWINDOWNAME="LASTMONTH_2019",
    )

    cols_lastyear = ["SentimentScore_LASTYear_2019", "SentimentScore_LASTYear_2020"]
    df_growth_lastyear = getSentimentGrowthOfTimeWindow(
        df_sentiment_2019LastYear,
        df_sentiment_2020LastYear,
        cols_lastyear,
        TimeWindowName="last year",
    )
    # df_growth_lastyear.show()

    cols_last6month = ["SentimentScore_LAST6MONTH_2019", "SentimentScore_LAST6MONTH_2020"]
    df_growth_last6month = getSentimentGrowthOfTimeWindow(
        df_sentiment_2019Last6Month,
        df_sentiment_2020Last6Month,
        cols_last6month,
        TimeWindowName="last 6 month",
    )


    cols_last3month = ["SentimentScore_LAST3MONTH_2019", "SentimentScore_LAST3MONTH_2020"]
    df_growth_last3month = getSentimentGrowthOfTimeWindow(
        df_sentiment_2019Last3Month,
        df_sentiment_2020Last3Month,
        cols_last3month,
        TimeWindowName="last 3 month",
    )

    cols_last1month = ["SentimentScore_LASTMONTH_2019", "SentimentScore_LASTMONTH_2020"]
    df_growth_lastmonth = getSentimentGrowthOfTimeWindow(
        df_sentiment_2019LastMonth,
        df_sentiment_2020LastMonth,
        cols_last1month,
        TimeWindowName="last month",
    )

    rank1D_sent = df_growth_lastyear.union(df_growth_last6month)
    rank1D_sent = rank1D_sent.union(df_growth_last3month)
    rank1D_sent = rank1D_sent.union(df_growth_lastmonth)

    return rank1D_sent
