class Tree:
    def __init__(self, value, parent=None):
        self.value = value
        self.parent = parent
        if(parent):
            parent.add_child(self)
        self.children = set()

    def set_parent(self, parent):
        self.parent = parent;

    def val(self):
        return self.value

    def add_child(self, child):
        self.children.add(child)

    def hello(self, indent=""):
        print(indent + self.val())
        for child in self.children:
            child.hello(indent + "  ")

    def depth_rec(self):
        if len(self.children) == 0:
            return 0
        else:
            return 1 + max([child.depth() for child in self.children])

    def depth(self):
        depth = 0
        wq = []
        path = []

        wq.append(self)
        count = 0
        while wq:
            count = count + 1
            #print([n.val() for n in wq], [n.val() for n in path])
            r = wq[-1]
            if path and r == path[-1]:
                if len(path) > depth:
                    depth = len(path)
                path.pop()
                wq.pop()
            else:
                path.append(r)
                for child in r.children:
                    wq.append(child)

        return depth - 1
        

