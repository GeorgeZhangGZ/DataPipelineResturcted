def combinationWithRetweets(Month,monthIter):
    pairs_weight = {}
    for phrases_pair,retweetslog in monthIter:
        for phrase_pair in phrases_pair:
            pairs_weight.setdefault(phrase_pair,1)
            pairs_weight[phrase_pair] +=int(retweetslog)
    return (Month,[(key,pairs_weight[key]) for key in pairs_weight.keys()])