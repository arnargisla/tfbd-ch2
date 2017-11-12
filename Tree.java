import java.util.Set;
import java.util.HashSet;
import java.util.Stack;

public class Tree{
  private Tree parent;
  private Set<Tree> children = new HashSet<Tree>();
  private String val;
  
  public Tree(String val){
    this.val = val;
  }

  public Tree(String val, Tree parent){
    this.val = val;
    this.parent = parent;
    parent.addChild(this);
  }

  public void hello(){
    this.hello("");
  }

  public void addChild(Tree child){
    this.children.add(child);
  }

  public void hello(String indent){
    System.out.println(indent + this.val);
    for(Tree child: this.children){
      child.hello(indent + "  ");
    }
  }

  public int depth(){ 
    int depth = 0;
    Stack<Tree> wq = new Stack<Tree>();
    Stack<Tree> path = new Stack<Tree>();

    wq.push(this);
    int count = 0;
    Tree r;
    while(!wq.empty()){
      count = count + 1;
      r = wq.peek();
      if(!path.empty() && r == path.peek()){
        if(path.size()>depth){
          depth = path.size();
        }
        path.pop();
        wq.pop();
      } else {
        path.push(r);
        for(Tree child: r.children){
          wq.push(child);
        }
      }
    }
    
    return depth - 1;
  }

  public static void main(String[] args){
    Tree t = new Tree("Groot");
    Tree t1 = new Tree("c1", t);
    Tree t2 = new Tree("c2", t);
    Tree t11 = new Tree("c11", t1);
    t.hello();
    System.out.println("Depth: " + t.depth());
    
  }
}
