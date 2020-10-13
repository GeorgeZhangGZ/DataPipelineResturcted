def sentiment_process(month, monthIter):
    """
    Input =>
    
    phrases : [string]
    Weighted_Sentiment: Float
    Likes : Int 
    Output =>
    ([(Vocabulary,Vocabulary_sentiment/vocabulary_like)])
    
    """
    phrases_list = []
    vocabulary_sentiment = {}
    vocabulary_likes = {}
    # time to go home to cock

    for phrases, Weighted_Sentiment, Likes in monthIter:
        for phrase in phrases:
            vocabulary_sentiment.setdefault(phrase, 0)
            vocabulary_likes.setdefault(phrase, 0)
            vocabulary_sentiment[phrase] += Weighted_Sentiment
            vocabulary_likes[phrase] += Likes
    return (
        month,
        [
            (key, vocabulary_sentiment[key] / (vocabulary_likes[key] + 1))
            for key in vocabulary_sentiment.keys()
        ],
    )