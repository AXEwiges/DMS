package common.zookeeper;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import common.meta.ClientInfo;
import common.meta.ClientInfoFactory;
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

    // Connect to zookeeper
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
    ClientInfo masterClient = ClientInfoFactory.from(
        new String(r));

    // Create node with unique name
    int uid = UUID.randomUUID().hashCode();
    while (true) {
      try {
        zk.create("/region_servers/" + uid,
            ClientInfoFactory.toString(clientInfo).getBytes(),
            OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      } catch (KeeperException e) {
        if (e.code() == Code.NODEEXISTS) {
          // Duplicated uid, try again
          uid = UUID.randomUUID().hashCode();
          continue;
        }
        throw e;
      }
      break;
    }

    connected = true;
    return masterClient.setUid(uid);
  }

  /**
   * For testing.
   *
   * @param args
   */
  public static void main(String[] args)
      throws IOException, InterruptedException, KeeperException {
    Client rsClient = new ClientRegionServerImpl();

    String host;
    int rpcPort;
    int socketPort;
    Scanner scanner = new Scanner(System.in);
    System.out.print("Hostname (won't be used, just for testing): ");
    host = scanner.nextLine();
    System.out.print("RPC Port (won't be used, just for testing): ");
    rpcPort = Integer.parseInt(scanner.nextLine());
    System.out.print("RPC Port (won't be used, just for testing): ");
    socketPort = Integer.parseInt(scanner.nextLine());

    ClientInfo master = rsClient.connect("127.0.0.1:2181",
        ClientInfoFactory.from(host, rpcPort, socketPort), 3000);
    System.out.println("Connected to " + master);
    Thread.sleep(1000000);
  }
}
