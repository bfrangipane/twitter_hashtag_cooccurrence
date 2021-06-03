

def main(next_hashtag):
    query = next_hashtag.replace('#', '%23') + ' -is:retweet lang:en'
    return query


if __name__ == "__main__":
    main()
