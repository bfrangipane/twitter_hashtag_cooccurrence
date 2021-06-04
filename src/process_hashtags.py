import json
import pandas as pd 
import re
import numpy as np
from collections import Counter
import operator


def load_tweets(file):
    with open(file) as f:
        tweets = json.load(f)
    return tweets


def flatten_tweet(tweet):
    tweet_flattened = tweet.copy() 
    del tweet_flattened['public_metrics']
    tweet_flattened.update(tweet['public_metrics'])
    tweet_series = pd.Series(tweet_flattened)
    return tweet_series


def extract_hash_tags(text):
    return set(hashtag.upper() for hashtag in re.findall(r'\B#\w*[a-zA-Z]+\w*', text))


def remove_extras(text):
    return ' '.join([word for word in text.split() if not (word.startswith('@') or word.startswith('https://'))])


def process_tweets(tweets, next_hashtag):
    tweet_df = pd.DataFrame({v:flatten_tweet(t) for t, v in zip(tweets, range(len(tweets)))}).T
    tweet_df['processed_text'] = tweet_df['text'].apply(remove_extras)
    tweet_df['hashtags'] = tweet_df['processed_text'].apply(extract_hash_tags).apply(sorted)
    tweet_df['num_hashtags'] = tweet_df['hashtags'].apply(len)
    tweet_df['hashtag_searched'] = next_hashtag
    return tweet_df


def remove_duplicates(tweet_df):
    tweet_df_dedup = tweet_df.drop_duplicates(subset=['author_id'])
    tweet_df_dedup = tweet_df_dedup.drop_duplicates(subset=['processed_text'])
    return tweet_df_dedup


def update_hashtag_matrix(tweet_df):
    hashtag_df=pd.DataFrame()
    def hashtag_matrix_helper(hashtag_set):
        n = len(hashtag_set)
        for i in range(n):
            for j in range(min(i+1, n), n):
                ht_i = hashtag_set[i]
                ht_j = hashtag_set[j]
                if ht_i in hashtag_df.columns and ht_j in hashtag_df.columns:
                    hashtag_cnt = np.nan_to_num(hashtag_df.at[ht_i,ht_j])
                    hashtag_cnt += 1
                    hashtag_df.at[ht_i,ht_j] = hashtag_cnt
                    hashtag_df.at[ht_j,ht_i] = hashtag_cnt
                else:
                    hashtag_df.at[ht_i,ht_j] = 1
                    hashtag_df.at[ht_j,ht_i] = 1
    tweet_df['hashtags'].apply(hashtag_matrix_helper)
    return hashtag_df
    

def find_next_hashtag(tweet_df, hashtags_searched):
    hashtags_found = [ht for ht_set in tweet_df['hashtags'] for ht in ht_set if ht not in hashtags_searched]
    hashtags_counted = Counter(hashtags_found)
    hashtags_sorted = sorted(hashtags_counted.items(), key=operator.itemgetter(1), reverse=True)
    next_hashtag = hashtags_sorted[0][0]
    return next_hashtag
    

def main(json_files, hashtags_searched, tweet_df, next_hashtag):
    for json_file in json_files:
        new_tweet_data = load_tweets(json_file)
        new_tweets = new_tweet_data['data']
        new_tweet_df = process_tweets(new_tweets, next_hashtag)
        tweet_df = pd.concat([tweet_df, new_tweet_df])
    tweet_df = remove_duplicates(tweet_df)
    hashtag_df = update_hashtag_matrix(tweet_df)
    next_hashtag = find_next_hashtag(tweet_df, hashtags_searched)
    return next_hashtag, hashtag_df, tweet_df


if __name__ == "__main__":
    main()
