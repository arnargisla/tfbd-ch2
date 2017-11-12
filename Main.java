import java.sql.*;
//import Tree;


class Main{

  private int MAX_SUBREDDITS_TO_PROCESS;

  void processSubreddits(Connection c){
    int counter = 0;
    try {
      Statement stmt = c.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT id, name FROM subreddits");
      while( rs.next() ){
        counter++;
        String id = rs.getString("id");
        String name = rs.getString("name");
        System.out.println(id + " " + name);
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

  public Main(){
    Main(Integer.MAX_VALUE);
  }

  public Main(int limit){
    this.MAX_SUBREDDITS_TO_PROCESS = limit;
  }

  public static void main(String[] args){
    Main program = new Main(Integer.parseInt(args[0]));
    try {
      Class.forName("org.sqlite.JDBC");

      Connection c = DriverManager.getConnection("jdbc:sqlite:reddit.db");
      program.processSubreddits(c);

      c.close();
    } catch ( Exception e ) {
      System.err.println("error initializing db");
      System.err.println("Err: " + e.getMessage());
      System.exit(0);
    }

  }
}
