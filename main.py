from ChallengeHelper.Tree import Tree
import sqlite3
import sys
conn = sqlite3.connect("reddit.db")

NUMBER_OF_SUBREDDITS_TO_CONSIDER = 10000000000000000000000000000000000
try:
    NUMBER_OF_SUBREDDITS_TO_CONSIDER = int(sys.argv[1])
except IndexError:
    pass
except ValueError:
    pass

def preprocess_subreddit(cursor, subreddit_id, roots, nodes):
    referenced_parents = {}
    while True:
        rows = cursor.fetchmany()
        if not rows:
            break
        for comment_id, parent_id in cursor:
            is_top_level = False
            # top level comments have the prefix t3_
            if parent_id[1] == "3":
                is_top_level = True

            node = Tree(comment_id)
            if comment_id in referenced_parents:
                for child_id in referenced_parents[comment_id]:
                    node.add_child(nodes[child_id])
                    del referenced_parents[comment_id]
            
            parent_node = None
            if not is_top_level:
                if parent_id in nodes:
                    parent_node = nodes[parent_id]
                    parent_node.add_child(node)
                    node.set_parent(parent_node)
                else:
                    if parent_id in referenced_parents:
                        referenced_parents[parent_id].append(comment_id)
                    else:
                        referenced_parents[parent_id] = [comment_id]
            else:
                roots[comment_id] = node
            nodes[comment_id] = node


def calculate_subreddit_stats(sub_id, subreddit_name, roots, nodes):
    max_depth = 0
    count = 0
    depth_acc = 0
    for node in roots.values():
        d = node.depth()
        depth_acc += d
        count += 1
        if d > max_depth:
            max_depth = d

    average_depth = 0
    if(count != 0):
        average_depth = depth_acc * (1.0 / (1.0 * count))

    print(average_depth, sub_id, subreddit_name, len(nodes), len(roots), max_depth)
    sys.stdout.flush()
    


def process_subreddit(cursor, subreddit_id, subreddit_name):
    roots = {}
    nodes = {}
    preprocess_subreddit(cursor, subreddit_id, roots, nodes)
    calculate_subreddit_stats(subreddit_id, subreddit_name, roots, nodes)


def main2():
    count = 0
    c = conn.cursor()
    c.execute("SELECT id, name FROM subreddits")
    rows = c.fetchall()
    for subreddit_id, name in rows:
        count += 1
        c.execute("SELECT id, parent_id FROM comments WHERE subreddit_id = \"{}\"".format(subreddit_id))
        process_subreddit(c, subreddit_id, name)
        if count >= NUMBER_OF_SUBREDDITS_TO_CONSIDER:
            break


def main():
    c = conn.cursor()
    c.execute("SELECT id, parent_id, subreddit_id FROM comments")
    top_level_count = 0
    count = 0
    
    subreddits = {}
    def get_subreddit(subreddit_id):
        try:
            sub = subreddits[subreddit_id]
            return sub
        except KeyError:
            sub = {}
            sub["roots"] = {}
            sub["nodes"] = {}
            subreddits[subreddit_id] = sub
            return sub

            
    for comment_id, parent_id, subreddit_id in c:
        sub = get_subreddit(subreddit_id)
        is_top_level = False
        # top level comments have the prefix t3_ in parent
        if parent_id[1] == "3":
            is_top_level = True

        node = Tree(comment_id)
        
        parent_node = None
        if not is_top_level:
            try:
                parent_node = sub["nodes"][parent_id]
                parent_node.add_child(node)
                node.set_parent(parent_node)
            except KeyError:
                pass
        else:
            sub["roots"][comment_id] = node
        sub["nodes"][comment_id] = node
        
        count += 1
        if(count % 100000 == 0):
            #print("{}\r".format(count), end="")
            pass
        if(count > int(NUMBER_OF_ROWS_TO_CONSIDER)):
            break

    reply_count = count - top_level_count
    #print("Total: {}, top level: {}, replies: {}".format(count, top_level_count, reply_count))

    #print("Total number of subs: {}".format(len(subreddits)))

    for sub_id, sub in subreddits.items():
        roots = sub["roots"]
        nodes = sub["nodes"]
        max_depth = 0
        count = 0
        depth_acc = 0
        for node in roots.values():
            d = node.depth()
            depth_acc += d
            count += 1
            if d > max_depth:
                max_depth = d

        average_depth = 0
        if(count != 0):
            average_depth = depth_acc * (1.0 / (1.0 * count))

        #print("looking at sub: {}".format(sub_id))
        #print("roots: {}".format(len(roots)))
        #print("nodes: {}".format(len(nodes)))
        #print("Deepest comment for sub {} is {} comments deep".format(sub_id, max_depth))
        #print("Depth acc", depth_acc)
        #print("Average depth {:f}".format(average_depth))
        print(average_depth, sub_id, len(nodes), len(roots), max_depth)

    #t = Tree("Groot")
    #c1 = Tree("Baby1", t)
    #c2 = Tree("Baby2", t)
    #c11 = Tree("Tiny11", c1)
    #t.hello()
    #print(t.depth())
#
    #l = []
    #for i in range(5000000):
        #l.append(Tree(str(i)))

    #print("list is {} ling".format(len(l)))

if __name__ == "__main__":
    main2()
