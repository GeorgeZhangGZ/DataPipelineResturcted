def sentiment_process_2d(month, monthIter):
    """
    Input =>
    
    phrase_pair : [(string,string)]
    Weighted_Sentiment: Float
    Likes : Int 
    Output =>
    ([(Vocabulary_pairs,Vocabulary_sentiment/vocabulary_like)])
    
    """
    phrases_list = []
    vocabulary_sentiment = {}
    vocabulary_likes = {}
    # time to go home to cock 

    for phrases_pair, Weighted_Sentiment, Likes in monthIter:
        for phrase_pair in phrases_pair:
            # phrase_pair =tuple(phrase_pair)
            vocabulary_sentiment.setdefault(phrase_pair, 0)
            vocabulary_likes.setdefault(phrase_pair, 0)
            vocabulary_sentiment[phrase_pair] += Weighted_Sentiment
            vocabulary_likes[phrase_pair] += Likes
    return (
        month,
        [
            (key, vocabulary_sentiment[key] / (vocabulary_likes[key] + 1))
            for key in vocabulary_sentiment.keys()
        ],
    )