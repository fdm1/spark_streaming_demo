#!/usr/bin/python

import json
import os
import sys
from nltk.corpus import stopwords
from textblob import TextBlob

custom_stopwords = [w.strip() for w in os.environ.get('custom_stopwords', '').split(',')]
all_stopwords=set(stopwords.words('english') + custom_stopwords)

for line in sys.stdin:
    split_text = tuple(line.split(sys.argv[1]))
    if len(split_text) == 2:
        raw_text, cleaned_text = split_text
         
        
        filtered_text = " ".join(w for w in cleaned_text.split(" ")
                                  if (w not in all_stopwords
                                      and not w.startswith("#")))
        hashtags = list(set([w for w in raw_text.split(" ") if w.startswith("#")]))

        words = list(set([w for w in [w.strip() for w in filtered_text.split(" ")] if len(w) > 2]))
        payload = {"raw_text": raw_text,
                   "polarity": TextBlob(filtered_text).polarity,
                   "words": words,
                   "hashtags": hashtags}

        print(json.dumps(payload))

