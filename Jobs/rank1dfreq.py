from functions.rank1dfreq import *
from functions.constants import *



def getRank1dFreq(pairs,df_lastYearTrendingScore,df_last1monthTrendingScore,df_last3monthTrendingScore,df_last6monthTrendingScore):

    df_freq_2020LastYear = getFreqDfByTimeWindow(pairs, LAST_Year_YEAR2020, "LastYear_2020")
    df_freq_2019LastYear = getFreqDfByTimeWindow(pairs, LAST_Year_YEAR2019, "LastYear_2019")


    df_freq_2020Last6Month = getFreqDfByTimeWindow(
        pairs, LAST_SIX_MONTHS_YEAR2020, "Last6Month_2020"
    )
    df_freq_2019Last6Month = getFreqDfByTimeWindow(
        pairs, LAST_SIX_MONTHS_YEAR2019, "Last6Month_2019"
    )


    df_freq_2020Last3Month = getFreqDfByTimeWindow(
        pairs, LAST_THREE_MONTHS_YEAR2020, "Last3Month_2020"
    )
    df_freq_2019Last3Month = getFreqDfByTimeWindow(
        pairs, LAST_THREE_MONTHS_YEAR2019, "Last3Month_2019"
    )


    df_freq_2020LastMonth = getFreqDfByTimeWindow(
        pairs, LAST_ONE_MONTHS_YEAR2020, "LastMonth_2020"
    )
    df_freq_2019LastMonth = getFreqDfByTimeWindow(
        pairs, LAST_ONE_MONTHS_YEAR2019, "LastMonth_2019"
    )


    cols_lastyear = ["Frequency_LastYear_2020", "Frequency_LastYear_2019"]
    df_freq_growth_lastyear = getFrequencyGrowthOfTimeWindow(
        df_freq_2019LastYear,
        df_freq_2020LastYear,
        cols_lastyear,
        TimeWindowName="last year",
    )
    # df_growth_lastyear.show()

    cols_last6month = ["Frequency_Last6Month_2020", "Frequency_Last6Month_2019"]
    df_freq_growth_last6month = getFrequencyGrowthOfTimeWindow(
        df_freq_2019Last6Month,
        df_freq_2020Last6Month,
        cols_last6month,
        TimeWindowName="last 6 month",
    )

    cols_last3month = ["Frequency_Last3Month_2020", "Frequency_Last3Month_2019"]
    df_freq_growth_last3month = getFrequencyGrowthOfTimeWindow(
        df_freq_2019Last3Month,
        df_freq_2020Last3Month,
        cols_last3month,
        TimeWindowName="last 3 month",
    )

    cols_last1month = ["Frequency_LastMonth_2020", "Frequency_LastMonth_2019"]
    df_freq_growth_lastmonth = getFrequencyGrowthOfTimeWindow(
        df_freq_2019LastMonth,
        df_freq_2020LastMonth,
        cols_last1month,
        TimeWindowName="last month",
    )

    df_freq_growth_lastyear = df_freq_growth_lastyear.join(df_lastYearTrendingScore,['Topic','Category2'],how='left')
    df_freq_growth_last6month = df_freq_growth_last6month.join(df_last6monthTrendingScore,['Topic','Category2'],how='left')
    df_freq_growth_last3month = df_freq_growth_last3month.join(df_last3monthTrendingScore,['Topic','Category2'],how='left')
    df_freq_growth_lastmonth = df_freq_growth_lastmonth.join(df_last1monthTrendingScore,['Topic','Category2'],how='left')
    rank1D_freq = df_freq_growth_lastyear.union(df_freq_growth_last6month)
    rank1D_freq = rank1D_freq.union(df_freq_growth_last3month)
    rank1D_freq = rank1D_freq.union(df_freq_growth_lastmonth)
    return rank1D_freq