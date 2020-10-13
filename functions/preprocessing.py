
import re
def ner_extraction(text,nlp):
    doc = nlp(text)
    entities = []
    for ent in doc.ents:
        if ent.ent_id_ == "":
            entities.append(ent.text)
        else:
            entities.append(ent.ent_id_)
    if entities == []:
        return ["empty"]
    else:
        return list(set(entities))


def transform_number(x):
    try:
        if "K" in str(x):
            return int(float(x[:-1]) * 1000)
        if "M" in str(x):
            return int(float(x[:-1]) * 1000000)
        else:
            return int(float(x))
    except:
        return int(0)
    

def extractkeyword(url):
    try:
        result = (
            re.search("searchq=(.+) until", url.replace("?", "").replace("%20", " "))
            .group(1)
            .replace(" lang%3Aen", "")
            .strip()
        )
        return result
    except:
        return None


def getCategory2(keyword):
    SODA = ["fizzy drink", "soda", "sparkling water"]
    TONIC = ["tonic"]
    GINGERALE = ["ginger ale", "coke", "pop"]
    if keyword in SODA:
        return "soda"
    if keyword in TONIC:
        return "tonic"
    if keyword in GINGERALE:
        return "ginger ale"

def checkempty(phrasesList):
    if phrasesList == ['empty']:
        return int(1)
    else:
        return int(0)