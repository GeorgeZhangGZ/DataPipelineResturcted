from functions.util import monthColumnGenerator
LAST_Year_YEAR2020 = [
    (2019, 6),
    (2019, 7),
    (2019, 8),
    (2019, 9),
    (2019, 10),
    (2019, 11),
    (2019, 12),
    (2020, 1),
    (2020, 2),
    (2020, 3),
    (2020, 4),
    (2020, 5),
]

LAST_Year_YEAR2019 = [
    (2018, 6),
    (2018, 7),
    (2018, 8),
    (2018, 9),
    (2018, 10),
    (2018, 11),
    (2018, 12),
    (2019, 1),
    (2019, 2),
    (2019, 3),
    (2019, 4),
    (2019, 5),
]

LAST_SIX_MONTHS_YEAR2020 = [
    (2019, 12),
    (2020, 1),
    (2020, 2),
    (2020, 3),
    (2020, 4),
    (2020, 5),
]
LAST_SIX_MONTHS_YEAR2019 = [
    (2018, 12),
    (2019, 1),
    (2019, 2),
    (2019, 3),
    (2019, 4),
    (2019, 5),
]

LAST_THREE_MONTHS_YEAR2020 = [(2020, 3), (2020, 4), (2020, 5)]
LAST_THREE_MONTHS_YEAR2019 = [(2019, 3), (2019, 4), (2019, 5)]

LAST_ONE_MONTHS_YEAR2020 = [(2020, 5)]
LAST_ONE_MONTHS_YEAR2019 = [(2019, 5)]



LAST_Year_YEAR2020_STRING_LIST = monthColumnGenerator("Frequency", LAST_Year_YEAR2020)
LAST_Year_YEAR2019_STRING_LIST = monthColumnGenerator("Frequency", LAST_Year_YEAR2019)

LAST_SIX_MONTH_YEAR2020_STRING_LIST = monthColumnGenerator(
    "Frequency", LAST_SIX_MONTHS_YEAR2020
)
LAST_SIX_MONTH_YEAR2019_STRING_LIST = monthColumnGenerator(
    "Frequency", LAST_SIX_MONTHS_YEAR2019
)

LAST_THREE_MONTH_YEAR2020_STRING_LIST = monthColumnGenerator(
    "Frequency", LAST_THREE_MONTHS_YEAR2020
)
LAST_THREE_MONTH_YEAR2019_STRING_LIST = monthColumnGenerator(
    "Frequency", LAST_THREE_MONTHS_YEAR2019
)

LAST_ONE_MONTH_YEAR2020_STRING_LIST = monthColumnGenerator(
    "Frequency", LAST_ONE_MONTHS_YEAR2020
)
LAST_ONE_MONTH_YEAR2019_STRING_LIST = monthColumnGenerator(
    "Frequency", LAST_ONE_MONTHS_YEAR2019
)