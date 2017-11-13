import java.sql.*;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class MainParallel{

  class WorkerThread implements Runnable {
    private String id;
    private String[] ids;
    private String name;
    private String[] names;
    private Connection connection;

    public WorkerThread(String[] ids, String[] names, Connection c){
      this.ids = ids;
      this.names = names;
      this.connection = c;
    }

    @Override
    public void run() {
      //System.out.println(Thread.currentThread().getName()+" Start. Command = "+id+" name: "+name);
      for(int i=0; i<ids.length; i++){
        this.id = this.ids[i];
        this.name = this.names[i];
        processCommand();
      }
      //System.out.println(Thread.currentThread().getName()+" End.");
    }

    private void processCommand() {
      try {
        Connection c = this.connection;
        Statement stmt = c.createStatement();
        stmt.setFetchSize(16000);
        String psString = "SELECT id, parent_id FROM comments WHERE subreddit_id = ?";
        PreparedStatement prepared = c.prepareStatement(psString);
        prepared.setString(1, id);
        ResultSet rs = prepared.executeQuery();
        process_subreddit(rs, id, name);
        rs.close();
        stmt.close();
      } catch (SQLException e) {
        System.err.println(e.getMessage());
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }
    }

    @Override
    public String toString(){
      return this.id + " " + this.name;
    }
  }

  private int MAX_SUBREDDITS_TO_PROCESS;

  void processSubreddits(Connection c){
    ExecutorService executor = Executors.newFixedThreadPool(8);
    int counter = 0;
    int batch_size = 500;
    try {
      Statement stmt = c.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT id, name FROM subreddits");
      while( rs.next() ){
        ArrayList<String> names = new ArrayList<String>(batch_size);
        ArrayList<String> ids = new ArrayList<String>(batch_size);
        int i = 0;
        do {
          String id = rs.getString("id");
          String name = rs.getString("name");
          ids.add(id);
          names.add(name);
          i++;
        } while ( i<batch_size && rs.next() );

        Runnable worker = new WorkerThread(ids.toArray(new String[ids.size()]), names.toArray(new String[names.size()]), c);
        executor.execute(worker);

        if(counter > this.MAX_SUBREDDITS_TO_PROCESS){
          break;
        }

      }
      executor.shutdown();
      while (!executor.isTerminated()) {
      }
      rs.close();
      stmt.close();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  static void process_subreddit(ResultSet rs, String subreddit_id, String subreddit_name) throws SQLException {
    HashMap<String, Tree> roots = new HashMap<String, Tree>();
    HashMap<String, Tree> nodes = new HashMap<String, Tree>();
    preprocess_subreddit(rs, subreddit_id, roots, nodes);
    calculate_subreddit_stats(subreddit_id, subreddit_name, roots, nodes);
  }

  static void preprocess_subreddit(ResultSet rs, String subreddit_id, HashMap<String, Tree> roots, HashMap<String, Tree> nodes) throws SQLException {
    HashMap<String, Set<String>> referenced_parents = new HashMap<String, Set<String>>();
    int counter = 0;
    while ( rs.next() ) {
      String comment_id = rs.getString("id");
      String parent_id = rs.getString("parent_id");
      boolean is_top_level = false;
      if (parent_id.charAt(1) == '3'){
        is_top_level = true;
      }

      Tree node = new Tree(comment_id);
      if( referenced_parents.containsKey(comment_id) ) {
        for( String child_id: referenced_parents.get(comment_id)){
          node.add_child(nodes.get(child_id));
        }
        referenced_parents.remove(comment_id);
      }
      Tree parent_node = null;
      if (!is_top_level) {
        if (nodes.containsKey(parent_id)) {
          parent_node = nodes.get(parent_id);
          parent_node.add_child(node);
          node.set_parent(parent_node);
        } else {
          if ( !referenced_parents.containsKey(parent_id) ){
            referenced_parents.put(parent_id, new HashSet<String>());
          }
          referenced_parents.get(parent_id).add(comment_id);
        }
      } else {
        roots.put(comment_id, node);
      }
      nodes.put(comment_id, node);
    }
  }

  static void calculate_subreddit_stats(String subreddit_id, String subreddit_name, HashMap<String, Tree> roots, HashMap<String, Tree> nodes){
    int max_depth = 0;
    int count = 0;
    int depth_acc = 0;
    for(Tree node: roots.values()){
      int d = node.depth();
      depth_acc += d;
      count += 1;
      if(d > max_depth) {
        max_depth = d;
      }
    }
    double average_depth = 0;
    if( count != 0 ) {
      average_depth = ((double)depth_acc) / ((double)count);
    }
    System.out.println(String.format("%.9f %s %s %d %d %d %s", average_depth, subreddit_id, subreddit_name, nodes.size(), roots.size(), max_depth, Thread.currentThread().getName()));

  }

  public MainParallel(){
    this(Integer.MAX_VALUE);
  }

  public MainParallel(int limit){
    this.MAX_SUBREDDITS_TO_PROCESS = limit;
  }

  public static void main(String[] args){
    MainParallel program = null;
    if(args.length < 1){
      program = new MainParallel();
    } else {
      program = new MainParallel(Integer.parseInt(args[0]));
    }
    try {
      Class.forName("org.sqlite.JDBC");

      Connection c = DriverManager.getConnection("jdbc:sqlite:reddit.db");
      c.setAutoCommit(false);
      program.processSubreddits(c);

      c.close();
    } catch ( Exception e ) {
      System.err.println("error initializing db");
      System.err.println("Err: " + e.getMessage());
      System.exit(0);
    }

  }
}
