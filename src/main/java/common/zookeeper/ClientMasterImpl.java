package common.zookeeper;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import common.meta.ClientInfo;
import common.meta.ClientInfoFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ClientMasterImpl implements Client {

  private ZooKeeper zk;
  private final ClientConnectionStrategy strategy;
  private boolean connected = false;

  public ClientMasterImpl(ClientConnectionStrategy strategy) {
    this.strategy = strategy;
  }

  @Override
  public ClientInfo connect(String zkHostPort, ClientInfo client,
      int sessionTimeout)
      throws IOException, InterruptedException, KeeperException {
    if (connected) {
      throw new IllegalStateException("Do not connect twice.");
    }

    zk = new ZooKeeper(zkHostPort, sessionTimeout, null);
    if (zk.exists("/master", null) == null) {
      // 注册 master
      zk.create("/master", ClientInfoFactory.toString(client).getBytes(),
          OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
    // 创建 /region_servers 节点
    if (zk.exists("/region_servers", null) == null) {
      zk.create("/region_servers", null, OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    }
    // 监控子节点列表
    EventCallback eventCallback = new EventCallback();
    zk.getChildren("/region_servers", eventCallback, eventCallback, null);

    connected = true;
    // 自己是主节点，返回自己
    return client;
  }

  /**
   * 处理子节点的连接和断连事件。
   * <p>
   * 实现细节：
   * <p>
   * 在 ZooKeeper 服务器中，每个 region server 是 /region_servers 节点下的子节点。 节点名称是连接到
   * ZooKeeper 中随机生成的，不会重复；节点内容是 region server 的地址 + 端口 （如 {@code
   * 192.168.1.1:5000}）。
   */
  private class EventCallback implements Watcher, ChildrenCallback {

    // the client cache
    private final Map<String, ClientInfo> clients = new HashMap<>();

    /**
     * Watcher 的回调，当子节点改变时调用
     *
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.NodeChildrenChanged) {
        if (event.getState() == KeeperState.SyncConnected) {
          zk.getChildren(event.getPath(), this, this, null);
        }
      }
    }

    /**
     * getChildren 查询的回调，返回结果时调用
     *
     * @param rc       返回值
     * @param path     查询路径
     * @param ctx
     * @param children 子节点列表
     */
    @Override
    public void processResult(int rc, String path, Object ctx,
        List<String> children) {
      if (Code.get(rc) == Code.OK) {
        System.out.println("In " + path + ": " + children);
        Set<String> disconnectedClients = new HashSet<>();

        for (String oldClient : clients.keySet()) {
          if (!children.contains(oldClient)) {
            // oldClient has disconnected
            strategy.onDisconnect(clients.get(oldClient));
            disconnectedClients.add(oldClient);
          }
        }

        // Remove disconnected clients from client cache
        clients.keySet().removeAll(disconnectedClients);

        for (String newClient : children) {
          if (!clients.containsKey(newClient)) {
            // we have a new client, add it to the client cache
            byte[] r;

            try {
              r = zk.getData(path + "/" + newClient, false, null);
            } catch (Exception e) {
              e.printStackTrace();
              continue;
            }

            ClientInfo cl = ClientInfoFactory.from(new String(r))
                .setUid(Integer.parseInt(newClient));
            clients.put(newClient, cl);

            // and execute the callback
            strategy.onConnect(cl);
          }
        }
      }
    }
  }

  /**
   * For testing.
   *
   * @param args
   */
  public static void main(String[] args)
      throws IOException, InterruptedException, KeeperException {
    Client masterClient = new ClientMasterImpl(new ClientConnectionStrategy() {
      @Override
      public void onConnect(ClientInfo clientInfo) {
        System.out.println(clientInfo + " connected, uid = " + clientInfo.uid);
      }

      @Override
      public void onDisconnect(ClientInfo clientInfo) {
        System.out.println(
            clientInfo + " disconnected, uid = " + clientInfo.uid);
      }
    });

    masterClient.connect("127.0.0.1:2181", ClientInfoFactory.from("1.2.3.4", 1),
        3000);
    Thread.sleep(1000000);
  }
}
