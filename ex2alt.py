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

    batch_size = 10000
    c.execute(''' SELECT * FROM comments; ''')
    while True:
        rows = c.fetchmany(batch_size)
        if not rows: break
        for row in rows:
            a_id = row[1]
            sr_id = row[2]
            if sr_id in authors:
                authors[sr_id].append(a_id)
            else:
                authors[sr_id] = [a_id]
    for sr in authors.keys():
        for osr in filter(lambda x: x != sr,authors.keys()):
            key = (sr,osr)
            if key in authors_in_common or tuple(reversed(key)) in authors_in_common:
                continue
            else:
                sr_authors = {a:1 for a in authors[sr]}
                osr_authors = {a:1 for a in authors[osr]}
                authors_in_common[key] = 0
                for a in sr_authors:
                    if a in osr_authors:
                        authors_in_common[key] += 1

    print_results(authors_in_common)


if __name__ == '__main__':
    conn = sqlite3.connect('reddit.db')
    c = conn.cursor()
    get_top_ten_authors_in_common()