#!/usr/bin/python

import json
import os
import sys
from nltk.corpus import stopwords
from nltk.stem.snowball import EnglishStemmer
from textblob import Word
from uuid import uuid4


stemmer = EnglishStemmer()
custom_stopwords = [w.strip() for w in os.environ.get('custom_stopwords', '').split(',')]
all_stopwords=set(stopwords.words('english') + custom_stopwords)


def generate_payload(input_str, splitter):
    split_text = tuple(input_str.split(splitter))
    if len(split_text) >= 2:
        raw_text, cleaned_text = split_text[:2]
        filtered_text = " ".join(w for w in cleaned_text.split(" ")
                                  if (w not in all_stopwords
                                      and not w.startswith("#")))
        words = list(set([Word(w).singularize().string for w in [stemmer.stem(w.strip()) for w in filtered_text.split(" ")] if len(w) > 2]))

        if words:
            hashtags = list(set([w for w in raw_text.split(" ") if w.startswith("#")])) or [""] 
            uuid = str(uuid4())
            is_retweet = len(split_text) == 3

            for word in words:
                for hashtag in hashtags:
                    yield json.dumps({"uuid": uuid, "word": word, "hashtag": hashtag, "is_retweet": is_retweet})
            # payload = {"uuid": str(uuid4()),
            #            "raw_text": raw_text,
            #            "is_retweet": len(split_text) == 3,
            #            "words": words,
            #            "hashtags": hashtags}
            #            # "polarity": TextBlob(filtered_text).polarity,
            # return json.dumps(payload)


for line in sys.stdin:
    payload = generate_payload(line, sys.argv[1])
    if payload:
        for word_hash in payload:
            print(word_hash)


#             return payload
#
#
# if __name__ == '__main__':
#     for line in sys.stdin:
#         print(json.dumps(generate_payload(line, sys.argv[1])))
#
