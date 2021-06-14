import prepare_query
import search_tweets
import process_hashtags
import pandas as pd
import json
from datetime import datetime
import sys
import os

def init_project(project_path):
    if not os.path.isdir(project_path):
        os.mkdir(project_path) 
        os.mkdir(project_path + '/data') 
        os.mkdir(project_path + '/output') 

def stop_iters(marginal_df, stopping_prob, iter_num, max_iters):
    if iter_num > max_iters:
        stop = True
    elif len(marginal_df) == 0:
        stop = False
    else:
        prob_df = marginal_df.drop(marginal_df.columns)
        prob_df.drop(['None'], inplace = True)
        prob = prob_df.max().max()
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

def save_data(hashtag_df, tweet_df, metadata, marginal_df, project_path):
    file_path = project_path + '/output/'
    with open(file_path + 'metadata.json', 'w') as outfile:
        json.dump(metadata, outfile, indent=4)
    hashtag_df.to_pickle(file_path + 'hashtag_df.pkl')
    tweet_df.to_pickle(file_path + 'tweet_df.pkl')
    marginal_df.to_pickle(file_path + 'marginal_df.pkl')

def load_data(project_path):
    path = '{}/output/metadata.json'.format(project_path)
    with open(path) as f:
        metadata = json.load(f)
    hashtags_searched = metadata['hashtags_searched']
    hashtag_df = pd.read_pickle('../output/hashtag_df.pkl')
    tweet_df = pd.read_pickle('../output/tweet_df.pkl')
    marginal_df = pd.read_pickle('../output/marginal_df.pkl')
    return metadata, hashtags_searched, hashtag_df, tweet_df, marginal_df

def continue_search(seed_hashtag='', project_path='../output', stopping_prob=.1, max_iters=5, sleep_period=2):
    metadata, hashtags_searched, hashtag_df, tweet_df, marginal_df = load_data(project_path)
    seed_hashtag = process_hashtags.find_next_hashtag(marginal_df) if seed_hashtag == '' else seed_hashtag
    next_hashtag, hashtag_df, hashtags_searched, tweet_df, marginal_df = begin_search(seed_hashtag, hashtag_df, hashtags_searched, tweet_df, stopping_prob, max_iters, sleep_period, metadata, marginal_df, project_path)
    return next_hashtag, hashtag_df, hashtags_searched, tweet_df, marginal_df

def begin_search(seed_hashtag, hashtag_df=pd.DataFrame(), hashtags_searched=[], tweet_df=pd.DataFrame(), stopping_prob=.1, max_iters=5, sleep_period=2, metadata=create_metadata(), marginal_df=pd.DataFrame(), project_path='../output'):
    init_project(project_path)
    next_hashtag = seed_hashtag
    iter_num = 1
    metadata = add_searchdata_to_metadata(metadata, seed_hashtag, stopping_prob, max_iters)
    while not stop_iters(marginal_df, stopping_prob, iter_num, max_iters):
        query = prepare_query.main(next_hashtag)
        json_files = search_tweets.main(query, next_hashtag, sleep_period, project_path)
        hashtags_searched.append(next_hashtag)
        metadata = update_metadata(metadata, next_hashtag, hashtags_searched, iter_num)
        next_hashtag, hashtag_df, tweet_df, marginal_df = process_hashtags.main(json_files, hashtags_searched, tweet_df, next_hashtag, marginal_df, stopping_prob)
        save_data(hashtag_df, tweet_df, metadata, marginal_df, project_path)
        if next_hashtag == '':
            break
        iter_num += 1
    return next_hashtag, hashtag_df, hashtags_searched, tweet_df, marginal_df

if __name__ == "__main__":
    method=sys.argv[1]
    seed_hashtag=sys.argv[2].upper()
    stopping_prob=float(sys.argv[3])
    max_iters=int(sys.argv[4])

    if method == 'begin':
        now_dt = datetime.now()
        now = now_dt.strftime("%Y%m%d%H%M%S")
        project_path = '../runs/' + seed_hashtag + now
        begin_search(seed_hashtag=seed_hashtag, stopping_prob=stopping_prob, max_iters=max_iters, project_path=project_path)
    elif method == 'continue':
        project_path = '../runs/' + sys.argv[5]
        continue_search(seed_hashtag=seed_hashtag, stopping_prob=stopping_prob, max_iters=max_iters, project_path=project_path)
