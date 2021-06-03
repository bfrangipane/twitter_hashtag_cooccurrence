
import requests
import os
import json
from datetime import datetime


def auth():
    return os.environ.get("TWITTER_BEARER_TOKEN")


def create_url(query, next_token):
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields=id,text,author_id,created_at,public_metrics,lang"
    pagination_field = "&next_token={}".format(next_token) if next_token!=None else ""
    result_field = "max_results=10"
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
    return tweet_json['meta']['next_token']


def main(query, next_hashtag):
    bearer_token = auth()
    i = 0
    json_files = []
    now_dt = datetime.now()
    now = now_dt.strftime("%Y%m%d%H%M%S")
    now_string = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    while i < 2:
        next_token = None if i == 0 else get_next_token(json_response)
        url = create_url(query, next_token)
        headers = create_headers(bearer_token)
        json_response = connect_to_endpoint(url, headers)
        print("{}: Searching for {}".format(now_string, next_hashtag))
        json_filename = next_hashtag + '_' + now + '_' + str(i)
        json_file = 'data/{}.json'.format(json_filename)
        json_files.append(json_file)
        write_to_file(json_response, json_file)
        i += 1
    return json_files


if __name__ == "__main__":
    main()