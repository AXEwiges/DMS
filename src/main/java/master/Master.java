package master;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.*;

import master.rpc.regionInfo;
import org.apache.thrift.TException;
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
import master.rpc.Master.Iface;

public class Master implements Watcher, Runnable {

    private ZooKeeper zk;
    public config _C;

    public Master() {
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

    HashMap<String, List<Integer>> tablesToRegions = new HashMap<>();//存储每张表存放在哪些region上
    HashMap<Integer, Integer> numsOfRegionTables = new HashMap<>();//存储每个region已经存放表的数量
    HashMap<Integer, regionInfo> regionsInfomation = new HashMap<>();//存储每个region的信息，key是region的uid

    /**
     * 实现Master接口方法
     */
    class MasterImpl implements Iface {
        @Override
        public List<regionInfo> getRegionsOfTable(String tableName, boolean isCreate, boolean isDrop) throws TException {
            List<Integer> regions;
            if(isCreate) {
                regions = findNMinRegion(3);
            }
            else {
                regions = tablesToRegions.get(tableName);
                if(isDrop) {
                    tablesToRegions.remove(tableName);
                    for(Integer i:regions) {
                        numsOfRegionTables.replace(i, numsOfRegionTables.get(i)-1);
                    }
                }
            }
            List<regionInfo> regionsINFO = new ArrayList<>();
            for(Integer i:regions) {
                regionsINFO.add(regionsInfomation.get(i));
            }
            return regionsINFO;
        }

        @Override
        public void finishCopyTable(String tableName, int uid) throws TException {
            tablesToRegions.get(tableName).add(uid);
        }
    }

    /**
     * 该函数用于实现负载均衡，通过每个region存储的表的数量来进行
     * @param N 表示要返回的region个数
     * @return  返回N个region
     */
    public List<Integer> findNMinRegion(int N) {
        List<Map.Entry<Integer, Integer>> list = new ArrayList<Map.Entry<Integer, Integer>>(numsOfRegionTables.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return (o1.getValue() - o2.getValue());
            }
        });

        List<Integer> NMinRegion = new ArrayList<>();

        for(int i = 0; i < N; i++) {
            Map.Entry<Integer, Integer> t = list.get(i);
            NMinRegion.add(t.getKey());
        }
        return NMinRegion;
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException {
        Master master = new Master();
        master.connectToZK();
        master.run();
    }
}
