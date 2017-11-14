import java.sql.*;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;


class Main{
  class WorkerThread implements Runnable {
    private String command;

    public WorkerThread(String s){
      this.command=s;
    }

    @Override
    public void run() {
      System.out.println(Thread.currentThread().getName()+" Start. Command = "+command);
      processCommand();
      System.out.println(Thread.currentThread().getName()+" End.");
    }

    private void processCommand() {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public String toString(){
      return this.command;
    }
  }

  private int MAX_SUBREDDITS_TO_PROCESS;

  void processSubreddits(Connection c){
    /*ExecutorService executor = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 10; i++) {
      Runnable worker = new WorkerThread("" + i);
      executor.execute(worker);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
    }
    System.out.println("Finished all threads");*/
    int counter = 0;
    try {
      Statement stmt = c.createStatement();
      Statement stmt2 = c.createStatement();
      stmt2.setFetchSize(1000);
      ResultSet rs = stmt.executeQuery("SELECT id, name FROM subreddits");
      String psString = "SELECT id, parent_id FROM comments WHERE subreddit_id = ?";
      PreparedStatement prepared = c.prepareStatement(psString);
      while( rs.next() ){
        counter++;
        String id = rs.getString("id");
        String name = rs.getString("name");
        prepared.setString(1, id);
        ResultSet rs2 = prepared.executeQuery();
        process_subreddit(rs2, id, name);
        if(counter > this.MAX_SUBREDDITS_TO_PROCESS){
          break;
        }
      }
      rs.close();
      stmt.close();
    } catch (SQLException e) {
      System.err.println(e.getMessage());
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  void process_subreddit(ResultSet rs, String subreddit_id, String subreddit_name) throws SQLException {
    HashMap<String, Tree> roots = new HashMap<String, Tree>();
    HashMap<String, Tree> nodes = new HashMap<String, Tree>();
    preprocess_subreddit(rs, subreddit_id, roots, nodes);
    calculate_subreddit_stats(subreddit_id, subreddit_name, roots, nodes);
  }

  void preprocess_subreddit(ResultSet rs, String subreddit_id, HashMap<String, Tree> roots, HashMap<String, Tree> nodes) throws SQLException {
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

  void calculate_subreddit_stats(String subreddit_id, String subreddit_name, HashMap<String, Tree> roots, HashMap<String, Tree> nodes){
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
    System.out.println(String.format("%.9f %s %s %d %d %d", average_depth, subreddit_id, subreddit_name, nodes.size(), roots.size(), max_depth));

  }

  public Main(){
    this(Integer.MAX_VALUE);
  }

  public Main(int limit){
    this.MAX_SUBREDDITS_TO_PROCESS = limit;
  }

  public static void main(String[] args){
    Main program = null;
    if(args.length < 1){
      program = new Main();
    } else {
      program = new Main(Integer.parseInt(args[0]));
    }
    try {
      Class.forName("org.sqlite.JDBC");

      Properties config = new Properties();
      config.setProperty("open_mode", "1"); // 1 = read only
      config.setProperty("synchronous", "off");
      config.setProperty("query_only", "true");

      Connection c = DriverManager.getConnection("jdbc:sqlite:reddit.db", config);
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
