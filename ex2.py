import sqlite3
import sys
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

    pc = 0
    c.execute(''' SELECT * FROM comments; ''')
    while True:
        rows = c.fetchmany()
        if not rows: break
        for row in rows:
            a_id = row[1]
            sr_id = row[2]
            if sr_id in authors:
                authors[sr_id][a_id] = 1
            else:
                authors[sr_id] = {a_id:1}
            pc += 1
            if pc % 1000000 == 0:
                print("Comments processed: " + str((pc / 53500000)*100) + "%")
                sys.stdout.flush()


    outercounter = 0
    counter = 0
    skipped = 0
    top10 = [0]*10
    tenthtop = 0
    for sr1 in authors.keys():
        outercounter += 1
        if outercounter % 100 == 0:
            print("complete {}% skipped: {}, total: {}, pct skipped {}".format(sr1, counter * 100.0/47000, skipped, counter, skipped * 100.0/counter))
            sys.stdout.flush()
        authors1 = authors[sr1]
        if len(authors[sr1]) < tenthtop:
            skipped += 1
            continue
        for sr2 in authors.keys():
            counter += 1
            if sr2 <= sr1:
                continue
            if len(authors[sr2]) < tenthtop:
                skipped += 1
                continue
            key = (sr1,sr2)
            if key in authors_in_common:
                continue
            else:
                authors2 = authors[sr2]
                common = 0
                for author1 in authors1:
                    if author1 in authors2:
                        common += 1
                if common > tenthtop:
                    top10 = sorted(top10[1:] + [common])
                    tenthtop = top10[0]

                authors_in_common[key] = common

    print_results(authors_in_common)


if __name__ == '__main__':
    conn = sqlite3.connect('reddit.db')
    c = conn.cursor()
    get_top_ten_authors_in_common()
