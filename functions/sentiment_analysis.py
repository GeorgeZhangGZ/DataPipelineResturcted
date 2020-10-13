import nltk
try:
    nltk.downloader.download('vader_lexicon')
except:
    pass

from nltk.sentiment.vader import SentimentIntensityAnalyzer


class VaderSentiment():
    """Predict sentiment scores using Vader.
    Tested using nltk.sentiment.vader and Python 3.6+
    https://www.nltk.org/_modules/nltk/sentiment/vader.html
    """

    def __init__(self):
        # pip install nltk
        # python > import nltk > nltk.download() > d > vader_lexicon
        self.vader = SentimentIntensityAnalyzer()

    def score(self, text):
        return self.vader.polarity_scores(text)['compound']