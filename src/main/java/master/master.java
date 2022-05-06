package master;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import config.config;

public class master implements Watcher, Runnable {

    private ZooKeeper zk;
    public config _C;

    public master() {
        this._C = new config();
        _C.loadYaml();
    }

    class EventCallback implements Watcher, ChildrenCallback {
        /**
         * Watcher 的回调，当子节点改变时调用
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
         * @param rc 返回值
         * @param path 查询路径
         * @param ctx
         * @param children 子节点列表
         */
        @Override
        public void processResult(int rc, String path, Object ctx,
                                  List<String> children) {
            if (Code.get(rc) == Code.OK) {
                System.out.println("In " + path + ": " + children);
            }
        }
    }

    /**
     * Connect to the ZooKeeper server.
     */
    public void connectToZK()
            throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(_C.network.ip + ':' + _C.network.port, _C.network.timeOut, (event) -> {
            System.out.println("Default watcher: " + event);
        });
        // Register master
        if (zk.exists("/master", null) == null) {
            zk.create("/master", _C.metadata.name.getBytes(), OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        // Listen for region servers
        EventCallback eventCallback = new EventCallback();
        zk.getChildren("/region_servers", eventCallback, eventCallback, null);
    }

    /**
     * Process any state changes.
     *
     * @param event the watched event
     */
    public void process(WatchedEvent event) {
        System.out.println(event);
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
            throws IOException, InterruptedException, KeeperException {
        master master = new master();
        master.connectToZK();
        master.run();
    } 
}
