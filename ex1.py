import sqlite3
from collections import Counter

def exctract_words(s):
    symbols = ['\n','`','~','!','@','#','$','%','^','&','*','(',')','_','-','+','=','{','[',']','}','|','\\',':',';','"',"'",'<','>','.','?','/',',']
    s = s.lower()
    for sym in symbols:
        s = s.replace(sym, " ")

    words = set()
    for w in s.split(" "):
        if len(w.replace(" ","")) > 0:
            words.add(w)
    return words

def print_progress(pc,bs,tr):
    total_rows = tr
    print(str(pc*bs) + " rows processed")
    print(str(((pc*bs)/total_rows)*100) + " % completed")

def print_results(subreddits):
    top_ten = Counter(subreddits).most_common(10)
    top_ten_subreddit_ids = [t[0] for t in top_ten]

    placeholder = '?'
    placeholders = ', '.join(placeholder for unused in top_ten_subreddit_ids)
    c.execute('SELECT name,id from subreddits where id IN (%s)' % placeholders, top_ten_subreddit_ids)

    top_ten_subreddit_names_and_ids = list(c.fetchall())
    for t in top_ten:
        for u in top_ten_subreddit_names_and_ids:
            if t[0] == u[1]:
                print(u[0] + ": " + str(t[1]) + " distinct words")

def get_top_ten_largest_vocabs():
    (total_number_of_rows,) = c.execute(''' SELECT COUNT(*) from comments; ''').fetchone()
    c.execute(''' SELECT subreddit_id, body from comments; ''')
    subreddits = {}
    progress_counter = 0
    batch_size = 10000
    while True:
        rows = c.fetchmany(batch_size)
        if not rows: break
        for row in rows:
            id, body = row[0], row[1]
            distinct_words_in_comment = {w:1 for w in exctract_words(body)}
            if id in subreddits:
                for key in distinct_words_in_comment:
                    if key not in subreddits[id]:
                        subreddits[id][key] = 1
            else:
                subreddits[id] = distinct_words_in_comment
        progress_counter += 1
        print_progress(progress_counter,batch_size,total_number_of_rows)
    print_results({k : len(list(v.keys())) for k,v in subreddits.items()})


if __name__ == '__main__':
    conn = sqlite3.connect('reddit.db')
    c = conn.cursor()
    get_top_ten_largest_vocabs()