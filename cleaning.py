import re

import pandas as pd

from nltk.corpus import stopwords

from nltk.tokenize import word_tokenize

from sklearn.feature_extraction.text import TfidfVectorizer


def remove_non_alphanumeric(text):
    return re.sub("[^a-zA-Z]", " ", text)


def convert_to_lowercase(text):
    return str(text).lower()


def tokenize(text):
    return word_tokenize(text)


def remove_stopwords(text):
    return [item for item in text if item not in set(stopwords.words("english"))]


def preprocess_df(df):
    df = df.dropna(how='any', axis=0)

    df["field_cleaned"] = df["field"].apply(remove_non_alphanumeric)
    df["field_cleaned"] = df["field_cleaned"].apply(convert_to_lowercase)
    df["field_cleaned"] = df["field_cleaned"].apply(tokenize)
    df["field_cleaned"] = df["field_cleaned"].apply(remove_stopwords)

    return df


# Data here
df = pd.read_csv("")
print(df.head())

df = preprocess_df(df)

tfidfvectorizer = TfidfVectorizer(analyzer='word')
tfidf_wm = tfidfvectorizer.fit_transform(df["blurb_cleaned"])
tfidf_tokens = tfidfvectorizer.get_feature_names()

print(tfidf_wm)
