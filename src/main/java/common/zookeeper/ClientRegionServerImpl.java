package common.zookeeper;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;

public class ClientRegionServerImpl implements Client {

  private ZooKeeper zk;
  private UUID uuid;
  private boolean connected = false;

  @Override
  public ClientInfo connect(String zkHostPort, ClientInfo clientInfo,
      int sessionTimeout)
      throws IOException, KeeperException, InterruptedException {
    if (connected) {
      throw new IllegalStateException("Do not connect twice.");
    }

    // Connected to zookeeper
    zk = new ZooKeeper(zkHostPort, sessionTimeout, null);
    // Make sure /region_servers node exists
    if (zk.exists("/region_servers", false) == null) {
      zk.create("/region_servers", null, OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    }

    // Read master
    byte[] r;
    try {
      r = zk.getData("/master", false, null);
    } catch (KeeperException e) {
      if (e.code() == Code.NONODE) {
        throw new IllegalStateException("The master is not connected.");
      }
      throw e;
    }
    ClientInfo masterClient = ClientInfo.from(new String(r));

    // Create node with unique name
    uuid = UUID.randomUUID();
    String path = "/region_servers/" + uuid;
    zk.create("/region_servers/" + uuid, clientInfo.toString().getBytes(),
        OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    connected = true;
    return masterClient;
  }

  /**
   * For testing.
   * @param args
   */
  public static void main(String[] args)
      throws IOException, InterruptedException, KeeperException {
    Client rsClient = new ClientRegionServerImpl();

    String host;
    int port;
    Scanner scanner = new Scanner(System.in);
    System.out.print("Hostname (won't be used, just for testing): ");
    host = scanner.nextLine();
    System.out.print("Port (won't be used, just for testing): ");
    port = Integer.parseInt(scanner.nextLine());

    ClientInfo master = rsClient.connect("127.0.0.1:2181", new ClientInfo(host, port), 3000);
    System.out.println("Connected to " + master);
    Thread.sleep(1000000);
  }
}
