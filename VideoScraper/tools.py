import pandas as pd
import itertools
import random

def read_csv(filename: str):
    query_terms = pd.read_csv(filename)
    query_terms.fillna('', inplace=True)
    terms_emotion = set(query_terms.Emotion.unique())
    terms_subject = set(query_terms.Subject.unique())
    terms_setting = set(query_terms.Setting.unique())
    for s in [terms_emotion, terms_subject, terms_setting]:
        if '' in s:
            s.remove('')

    all_queries = list(itertools.product(terms_emotion, terms_setting, terms_subject))
    random.shuffle(all_queries)

    return all_queries


