import sqlite3
import copy
from collections import Counter


def print_results(subreddits):
    top_ten = Counter(subreddits).most_common(10)
    top_ten_subreddit_pairs_ids = [t[0] for t in top_ten]
    top_ten_subreddit_flattened_ids = [y for x in top_ten_subreddit_pairs_ids for y in x]

    placeholder = '?'
    placeholders = ', '.join(placeholder for unused in top_ten_subreddit_flattened_ids)
    c.execute('SELECT name,id from subreddits where id IN (%s)' % placeholders, top_ten_subreddit_flattened_ids)

    top_ten_subreddit_pairs_names_and_ids = c.fetchall()
    sr_id_name_dict = {v:k for (k,v) in top_ten_subreddit_pairs_names_and_ids}
    for t in top_ten:
        for u in top_ten_subreddit_pairs_ids:
            if t[0] == u:
                print(sr_id_name_dict[u[0]] + " and " + sr_id_name_dict[u[1]] + ": " + str(t[1]) + " authors in common")

def get_top_ten_authors_in_common():
    authors = {}
    authors_in_common = {}

    list_of_subreddits = c.execute(''' SELECT id,name FROM subreddits; ''').fetchall()
    progress_counter = 0
    for sr in list_of_subreddits:
        sr_id,sr_name = sr
        c.execute(''' SELECT DISTINCT author_id FROM comments where subreddit_id = ? ''', [sr_id])
        while True:
            author_ids = c.fetchmany()
            if not author_ids: break
            for a_id in author_ids:
                if sr_id in authors:
                    authors[sr_id].append(a_id)
                else:
                    authors[sr_id] = []
        progress_counter += 1
        print("Processed " + str(progress_counter) + "/" + str(len(list_of_subreddits)) + " subreddits")
    for sr in authors.keys():
        for osr in filter(lambda x: x != sr,authors.keys()):
            key = (sr,osr)
            if key in authors_in_common or tuple(reversed(key)) in authors_in_common:
                continue
            else:
                authors_in_common[key] = len(set(authors[sr]).intersection(set(authors[osr])))

    print_results(authors_in_common)






if __name__ == '__main__':
    conn = sqlite3.connect('reddit.db')
    c = conn.cursor()
    get_top_ten_authors_in_common()