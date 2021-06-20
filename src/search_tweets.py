import time
import requests
import os
import json
from datetime import datetime

def auth():
    return os.environ.get("TWITTER_BEARER_TOKEN")

def create_url(query, next_token, num_tweets):
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields=id,text,author_id,created_at,public_metrics,lang"
    pagination_field = "&next_token={}".format(next_token) if next_token!=None else ""
    result_field = "max_results={}".format(num_tweets)
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}{}&{}".format(
        query, tweet_fields, pagination_field, result_field
    )
    return url

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params=None):
    response = requests.request("GET", url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def write_to_file(data, filename):
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=2)

def get_next_token(tweet_json):
    if 'next_token' in tweet_json['meta'].keys():
        next_token = tweet_json['meta']['next_token']
    else:
        next_token = 'No more tokens' 
    return next_token

def main(query, next_hashtag, sleep_period, project_path, num_tweets, num_searches):
    bearer_token = auth()
    i = 1
    json_files = []
    while i <= num_searches:
        now_dt = datetime.now()
        now = now_dt.strftime("%Y%m%d%H%M%S")
        now_string = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        next_token = None if i == 1 else get_next_token(json_response)
        if next_token == 'No more tokens':
            break
        url = create_url(query, next_token, num_tweets)
        headers = create_headers(bearer_token)
        json_response = connect_to_endpoint(url, headers)
        print("{}: Search #{} for {}".format(now_string, i, next_hashtag))
        json_filename = next_hashtag + '_' + now + '_' + str(i)
        json_file = project_path + '/data/{}.json'.format(json_filename)
        json_files.append(json_file)
        write_to_file(json_response, json_file)
        i += 1
        time.sleep(sleep_period)
    return json_files


if __name__ == "__main__":
    main()