from ChallengeHelper.Tree import Tree

t = Tree("Groot")
c1 = Tree("Baby1", t)
c2 = Tree("Baby2", t)
c11 = Tree("Tiny11", c1)
t.hello()
print(t.depth())

l = []
for i in range(53000000):
    if(i%1000000 == 0):
        print(i)
    l.append(Tree("lål"))

