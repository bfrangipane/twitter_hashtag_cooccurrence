import prepare_query
import search_tweets
import process_hashtags
import pandas as pd
import json
from datetime import datetime


def num_to_prob(col):
    return col/col.sum()


def stop(hashtag_df, hashtags_searched, stopping_prob, iter_num, max_iters):
    if iter_num > max_iters:
        stop = True
    elif hashtags_searched == []:
        stop = False
    else:
        prob_df = hashtag_df.apply(num_to_prob)
        prob = prob_df[hashtags_searched].drop(hashtags_searched).max().max()
        stop = (prob < stopping_prob)
    return stop


def create_metadata():
    metadata = {}
    metadata['init_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metadata['num_searches'] = 0
    metadata['searches'] = {}
    metadata['hashtags_searched'] = []
    return metadata


def add_searchdata_to_metadata(metadata, seed_hashtag, stopping_prob=.2, max_iters=100):
    searchdata = {}
    searchdata['init_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    searchdata['seed_hashtag'] = seed_hashtag
    searchdata['stopping_prob'] = stopping_prob
    searchdata['max_iters'] = max_iters
    searchdata['iters'] = {}
    metadata['num_searches'] += 1
    metadata['searches'][metadata['num_searches']] = searchdata
    return metadata


def add_iterdata_to_searchdata(next_hashtag, iter_num, searchdata):
    iterdata = {}
    iterdata['iter_num'] = iter_num
    iterdata['hashtag'] = next_hashtag
    iterdata['itertime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    searchdata['iters'][iter_num] = iterdata
    return searchdata


def update_metadata(metadata, next_hashtag, hashtags_searched, iter_num):
    searchdata = metadata['searches'][metadata['num_searches']]
    searchdata = add_iterdata_to_searchdata(next_hashtag, iter_num, searchdata)
    metadata['hashtags_searched'] = hashtags_searched
    return metadata


def save_data(hashtag_df, tweet_df, metadata):
    with open('../output/metadata.json', 'w') as outfile:
        json.dump(metadata, outfile, indent=4)
    hashtag_df.to_pickle('../output/hashtag_df.pkl')
    tweet_df.to_pickle('../output/tweet_df.pkl')


def continue_search(seed_hashtag='', path_to_metadata='../output', stopping_prob=.2, max_iters=100, sleep_period=2):
    path = '{}/metadata.json'.format(path_to_metadata)
    with open(path) as f:
        metadata = json.load(f)
    hashtags_searched = metadata['hashtags_searched']
    hashtag_df = pd.read_pickle('../output/hashtag_df.pkl')
    tweet_df = pd.read_pickle('../output/tweet_df.pkl')
    seed_hashtag = process_hashtags.find_next_hashtag(tweet_df, hashtags_searched) if seed_hashtag == '' else seed_hashtag
    metadata = add_searchdata_to_metadata(metadata, seed_hashtag, stopping_prob, max_iters)
    next_hashtag, hashtag_df, hashtags_searched, tweet_df = run(seed_hashtag, hashtag_df, hashtags_searched, tweet_df, stopping_prob, max_iters, sleep_period, metadata)
    return next_hashtag, hashtag_df, hashtags_searched, tweet_df


def run(seed_hashtag, hashtag_df=pd.DataFrame(), hashtags_searched=[], tweet_df=pd.DataFrame(), stopping_prob=.2, max_iters=100, sleep_period=2, metadata=create_metadata()):
    next_hashtag = seed_hashtag
    iter_num = 1
    metadata = add_searchdata_to_metadata(metadata, seed_hashtag, stopping_prob, max_iters)
    while not stop(hashtag_df, hashtags_searched, stopping_prob, iter_num, max_iters):
        query = prepare_query.main(next_hashtag)
        json_files = search_tweets.main(query, next_hashtag, sleep_period)
        hashtags_searched.append(next_hashtag)
        metadata = update_metadata(metadata, next_hashtag, hashtags_searched, iter_num)
        next_hashtag, hashtag_df, tweet_df = process_hashtags.main(json_files, hashtags_searched, tweet_df, next_hashtag)
        save_data(hashtag_df, tweet_df, metadata)
        iter_num += 1
    return next_hashtag, hashtag_df, hashtags_searched, tweet_df


if __name__ == "__main__":
    run()


# to do:
# add the value at the instersection to the number for hashtag df?
# -- the benefit is to use it to get accurate markov probs, which is essential
# -- but need to make changes in the next hashtag function or the stopping criteria

# wrap up metadata > searchdata > iterdata
# each need to have complete picture of subprocesses including hashtags_searched
# print functions for iterations
# need some init.py to create directories
# open search tweets to 100 tweets per search
# add total tweets per hashtag to metadata?