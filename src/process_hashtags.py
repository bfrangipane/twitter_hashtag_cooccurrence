import json
import pandas as pd 
import re
import numpy as np
from collections import Counter

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
    
def find_next_hashtag(marginal_df, stopping_prob, hashtags_searched):
    if len(marginal_df) == 0:
        return '', '', None # need to add more return values 
    prob_df = marginal_df.drop(hashtags_searched, errors='ignore')
    prob_df.drop(['None'], inplace = True)
    m, n = prob_df.shape
    offset_df = pd.DataFrame(np.reshape(list(range(n-1,-1,-1))*m, [m, n]))
    offset_df.columns = prob_df.columns
    offset_df.index = prob_df.index
    rank_df = offset_df + prob_df[prob_df >= stopping_prob]
    next_hashtag = rank_df.max(axis = 1).sort_values(ascending = False).index[0]
    parent_hashtag = rank_df.loc[next_hashtag].sort_values(ascending=False).index[0]
    hashtag_prob = prob_df.at[next_hashtag, parent_hashtag]
    return next_hashtag, parent_hashtag, hashtag_prob

def marginal_probs(tweet_df, hashtags_searched):
    marginal_df = pd.DataFrame()
    if len(tweet_df) == 0:
        return marginal_df
    if len(hashtags_searched) == 0:
        return marginal_df
    for hashtag in hashtags_searched:
        tweets_in_search_df = tweet_df[tweet_df['hashtag_searched']==hashtag]
        num_tweets = len(tweets_in_search_df)
        if num_tweets == 0:
            continue
        hashtag_list = [ht for ht_set in tweets_in_search_df['hashtags'] for ht in ht_set]
        hashtag_counts = Counter(hashtag_list)
        for ht, num in hashtag_counts.items():
            marginal_df.at[ht, hashtag] = num/num_tweets
        num_solo_hashtag_tweets = sum(tweets_in_search_df['num_hashtags']==1)
        marginal_df.at['None', hashtag] = num_solo_hashtag_tweets/num_tweets
    marginal_df.fillna(0, inplace=True)
    return marginal_df

def print_search_metrics(marginal_df, hashtag, parent_hashtag, hashtag_prob, top_n=10):
    print('{} <-- {}: {}%'.format(hashtag, parent_hashtag, round(hashtag_prob*100, 2)))
    if len(marginal_df) == 0:
        print('Search metrics empty for hashtag: {}'.format(hashtag))
        return
    margins_sorted = marginal_df[marginal_df[hashtag] > 0].sort_values(by=hashtag, ascending=False)
    i = 0
    top_n = min(top_n, len(margins_sorted)) 
    print ("Top {} co-occurrences for {}:".format(top_n, hashtag))
    while i < top_n:
        ht = margins_sorted.index[i]
        prob = margins_sorted[hashtag][i]
        if hashtag != ht:
            print('  {} --> {}: {}%'.format(hashtag, ht, round(prob*100, 2)))
        i += 1

def main(json_files, hashtags_searched, tweet_df, next_hashtag, marginal_df, stopping_prob, parent_hashtag, hashtag_prob):
    for json_file in json_files:
        new_tweet_data = load_tweets(json_file)
        if new_tweet_data['meta']['result_count'] == 0:
            continue
        new_tweets = new_tweet_data['data']
        new_tweet_df = process_tweets(new_tweets, next_hashtag)
        tweet_df = pd.concat([tweet_df, new_tweet_df])
    tweet_df = remove_duplicates(tweet_df)
    sub_marginal_df = marginal_probs(tweet_df, [next_hashtag])
    print_search_metrics(sub_marginal_df, next_hashtag, parent_hashtag, hashtag_prob)
    marginal_df = marginal_probs(tweet_df, hashtags_searched)
    next_hashtag, parent_hashtag, hashtag_prob = find_next_hashtag(marginal_df, stopping_prob, hashtags_searched)
    return next_hashtag, tweet_df, marginal_df, parent_hashtag, hashtag_prob


if __name__ == "__main__":
    main()
