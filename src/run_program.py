import prepare_query
import search_tweets
import process_hashtags
import pandas as pd
import json
from datetime import datetime
import time 


def num_to_prob(col):
    return col/col.sum()


def stop(hashtag_df, hashtags_searched, stopping_prob, iter_num, max_iters):
    if iter_num > max_iters:
        stop = False
    elif hashtags_searched == []:
        stop = False
    else:
        prob_df = hashtag_df.apply(num_to_prob)
        prob = prob_df[hashtags_searched].drop(hashtags_searched).max().max()
        stop = (prob >= stopping_prob)
    return stop, prob


def create_metadata(seed_hashtag, stopping_prob=.2, max_iters=100):
    metadata = {}
    metadata['iters'] = {}
    metadata['init_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metadata['seed_hashtag'] = seed_hashtag
    metadata['stopping_prob'] = stopping_prob
    metadata['max_iters'] = max_iters
    return metadata


def write_to_file(seed_hashtag, next_hashtag, hashtag_df, hashtags_searched, tweet_df, stopping_prob, iter_num, max_iters, metadata):
    rundata = {}
    rundata['runtime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rundata['hashtag'] = next_hashtag
    rundata['iter_num'] = iter_num
    metadata['iter'][iter_num] = rundata
    metadata['hashtags_searched'] = hashtags_searched
    metadata['num_iters'] = iter_num
    with open('output/metadata.txt', 'w') as outfile:
        json.dump(metadata, outfile, indent=2)
    hashtag_df.to_csv('output/hashtag_df.csv')
    tweet_df.to_csv('output/tweet_df.csv')
    return metadata


def continue_search(seed_hashtag='', path_to_metadata='../output', stopping_prob=.2, max_iters=100, sleep_period=2):
    path= '{}/metadata.txt'.format(path_to_metadata)
    with open(path) as f:
        metadata = json.load(f)
    hashtags_searched = metadata['hashtags_searched']
    hashtag_df = pd.read_csv('../output/hashtag_df.csv')
    tweet_df = pd.read_csv('../output/tweet_df.csv')
    next_hashtag, hashtag_df, hashtags_searched, tweet_df = run(seed_hashtag, hashtag_df=pd.DataFrame(), hashtags_searched=[], tweet_df=pd.DataFrame(), stopping_prob=.2, max_iters=100, sleep_period=2)
    seed_hashtag = process_hashtags.find_next_hashtag(tweet_df, hashtags_searched) if seed_hashtag == '' else seed_hashtag
    return next_hashtag, hashtag_df, hashtags_searched, tweet_df


def run(seed_hashtag, hashtag_df=pd.DataFrame(), hashtags_searched=[], tweet_df=pd.DataFrame(), stopping_prob=.2, max_iters=100, sleep_period=2):
    next_hashtag = seed_hashtag
    iter_num = 1
    metadata = create_metadata(seed_hashtag, stopping_prob, max_iters)
    while not stop(hashtag_df, hashtags_searched, stopping_prob, iter_num, max_iters, sleep_period):
        query = prepare_query.main(next_hashtag)
        json_files = search_tweets.main(query, next_hashtag)
        hashtags_searched.append(next_hashtag)
        next_hashtag, hashtag_df, tweet_df = process_hashtags.main(json_files, hashtags_searched, tweet_df, next_hashtag)
        metadata = write_to_file(seed_hashtag, next_hashtag, hashtag_df, hashtags_searched, tweet_df, stopping_prob, iter_num, max_iters, metadata)
        iter_num += 1
        time.sleep(sleep_period)
    return next_hashtag, hashtag_df, hashtags_searched, tweet_df


if __name__ == "__main__":
    run()


# to do:
# add the value at the instersection to the number for hashtag df?

# need some init.py to create directories
