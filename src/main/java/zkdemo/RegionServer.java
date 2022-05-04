package zkdemo;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.Scanner;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class RegionServer implements Runnable {

  private ZooKeeper zk;
  public final String hostPort;
  public final String name;
  public final int sessionTimeout;

  public RegionServer(String hostPort, String name) {
    this(hostPort, name, 3000);
  }

  public RegionServer(String hostPort, String name, int sessionTimeout) {
    this.hostPort = hostPort;
    this.name = name;
    this.sessionTimeout = sessionTimeout;
  }

  /**
   * Connect to the ZooKeeper server.
   */
  public void connectToZK()
      throws IOException, InterruptedException, KeeperException {
    zk = new ZooKeeper(hostPort, sessionTimeout, (event) -> {
      System.out.println("Default watcher: " + event);
    });
    // Register this region server
    Stat stat = zk.exists("/region_servers", false);
    if (stat == null) {
      zk.create("/region_servers", null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zk.create("/region_servers/" + name, name.getBytes(), OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL);
    // Read master
    // TODO
  }

  public void run() {
    try {
      synchronized (this) {
        wait();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args)
      throws InterruptedException, IOException, KeeperException {
    String name;
    System.out.println("Input the name of the region server: ");
    Scanner scanner = new Scanner(System.in);
    name = scanner.nextLine();
    RegionServer rs = new RegionServer("127.0.0.1:2181", name);
    rs.connectToZK();
    rs.run();
  }
}
