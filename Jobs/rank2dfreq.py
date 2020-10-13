from functions.rank2dfreq import *
from functions.constants import *
def getRank2dFreq(freq_2d_pairs,df_lastYearTrendingScore2d,df_last6monthTrendingScore2d,df_last3monthTrendingScore2d,df_last1monthTrendingScore2d):
    df_2d_freq_2020LastYear = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_Year_YEAR2020,
    TIMEWINDOWNAME="LASTYear_2020",
    )
    df_2d_freq_2019LastYear = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_Year_YEAR2019,
    TIMEWINDOWNAME="LASTYear_2019",
    )

    df_2d_freq_2020Last6Month = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_SIX_MONTHS_YEAR2020,
    TIMEWINDOWNAME="LAST6MONTH_2020",
    )
    df_2d_freq_2019Last6Month = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_SIX_MONTHS_YEAR2019,
    TIMEWINDOWNAME="LAST6MONTH_2019",
    )


    df_2d_freq_2020Last3Month = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_THREE_MONTHS_YEAR2020,
    TIMEWINDOWNAME="LAST3MONTH_2020",
    )
    df_2d_freq_2019Last3Month = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_THREE_MONTHS_YEAR2019,
    TIMEWINDOWNAME="LAST3MONTH_2019",
    )

    df_2d_freq_2020LastMonth = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_ONE_MONTHS_YEAR2020,
    TIMEWINDOWNAME="LASTMONTH_2020",
    )
    df_2d_freq_2019LastMonth = getFreqDf2DByTimeWindow(
    rdd=freq_2d_pairs,
    TIMEWINDOW=LAST_ONE_MONTHS_YEAR2019,
    TIMEWINDOWNAME="LASTMONTH_2019",
    )
    cols_lastyear = ["Frequency_LASTYear_2019", "Frequency_LASTYear_2020"]
    df_freq_growth_lastyear_2d = getFrequencyGrowthOfTimeWindow2d(
        df_2d_freq_2019LastYear,
        df_2d_freq_2020LastYear,
        cols_lastyear,
        TimeWindowName="last year",
    )

    cols_last6month = ["Frequency_LAST6MONTH_2019", "Frequency_LAST6MONTH_2020"]
    df_freq_growth_last6month_2d = getFrequencyGrowthOfTimeWindow2d(
        df_2d_freq_2019Last6Month,
        df_2d_freq_2020Last6Month,
        cols_last6month,
        TimeWindowName="last 6 month",
    )

    # df_growth_lastyear.show()

    cols_last3month = ["Frequency_LAST3MONTH_2019", "Frequency_LAST3MONTH_2020"]
    df_freq_growth_last3month_2d = getFrequencyGrowthOfTimeWindow2d(
        df_2d_freq_2019Last3Month,
        df_2d_freq_2020Last3Month,
        cols_last3month,
        TimeWindowName="last 3 month",
    )

    cols_last1month = ["Frequency_LASTMONTH_2019", "Frequency_LASTMONTH_2020"]
    df_freq_growth_lastmonth_2d = getFrequencyGrowthOfTimeWindow2d(
        df_2d_freq_2019LastMonth,
        df_2d_freq_2020LastMonth,
        cols_last1month,
        TimeWindowName="last month",
    )


    df_freq_growth_lastyear_2d = df_freq_growth_lastyear_2d.join(df_lastYearTrendingScore2d,['Topic','Topic2','Category2'],how='left')
    df_freq_growth_last6month_2d = df_freq_growth_last6month_2d.join(df_last6monthTrendingScore2d,['Topic','Topic2','Category2'],how='left')
    df_freq_growth_last3month_2d = df_freq_growth_last3month_2d.join(df_last3monthTrendingScore2d,['Topic','Topic2','Category2'],how='left')
    df_freq_growth_lastmonth_2d = df_freq_growth_lastmonth_2d.join(df_last1monthTrendingScore2d,['Topic','Topic2','Category2'],how='left')

    # df_growth_lastmonth_2d.show()
    rank2D_freq = df_freq_growth_lastyear_2d.union(df_freq_growth_last6month_2d)
    rank2D_freq = rank2D_freq.union(df_freq_growth_last3month_2d)
    rank2D_freq = rank2D_freq.union(df_freq_growth_lastmonth_2d)
    return rank2D_freq
