package master;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.*;

import master.rpc.cacheTable;
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
import com.alibaba.fastjson.JSON;
import master.rpc.Master.Iface;

public class Master implements Watcher, Runnable {

    private ZooKeeper zk;
    public config _C;

    HashMap<String, List<Integer>> tablesToRegions = new HashMap<>();//存储每张表存放在哪些region上
    HashMap<Integer, List<String>> regionsToTables = new HashMap<>();//存储每个region已经存放表
    HashMap<Integer, cacheTable> regionsInfomation = new HashMap<>();//存储每个region的信息，key是region的uid

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
                /**
                 * 对region新增和断开连接进行处理
                 */
                int num1 = children.size();
                int num2 = regionsInfomation.size();
                //新增节点
                if(num1>num2) {
                    for (String child : children) {
                        cacheTable cache = JSON.parseObject(child, cacheTable.class);
                        if(!regionsInfomation.containsKey(cache.uid)) {
                            regionsInfomation.put(cache.uid, cache);
                            regionsToTables.put(cache.uid, new ArrayList<>());
                            //这边考虑负载均衡可以给新来的region分配几个表
                        }
                    }
                }
                else if(num1<num2) {
                    Set<Integer> set = regionsInfomation.keySet();
                    for (String child : children) {
                        cacheTable cache = JSON.parseObject(child, cacheTable.class);
                        if(set.contains(cache.uid)) {
                            set.remove(cache.uid);
                        }
                        for(Integer uid:set) {
                            List<String> list = regionsToTables.get(uid);
                            for(String name:list) {
                                //TODO 调用region接口 给region发出复制表指令
                                tablesToRegions.get(name).remove(uid);
                            }
                            regionsToTables.remove(uid);
                            regionsInfomation.remove(uid);
                        }
                    }
                }
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

    /**
     * 实现Master接口方法
     */
    class MasterImpl implements Iface {
        @Override
        public List<cacheTable> getRegionsOfTable(String tableName, boolean isCreate, boolean isDrop) throws TException {
            List<Integer> regions;
            if(isCreate) {
                regions = findNMinRegion(3);
                tablesToRegions.put(tableName, regions);
                for(Integer i:regions) {
                    List<String> tables = regionsToTables.get(i);
                    tables.add(tableName);
                }
            }
            else {
                regions = tablesToRegions.get(tableName);
                if(isDrop) {
                    for(Integer i:regions) {
                        List<String> tables = regionsToTables.get(i);
                        tables.remove(tableName);
                    }
                    tablesToRegions.remove(tableName);
                }
            }
            List<cacheTable> regionsINFO = new ArrayList<>();
            for(Integer i:regions) {
                regionsINFO.add(regionsInfomation.get(i));
            }
            return regionsINFO;
        }

        @Override
        public void finishCopyTable(String tableName, int uid) throws TException {
            tablesToRegions.get(tableName).add(uid);
            regionsToTables.get(uid).add(tableName);
        }
    }

    /**
     * 该函数用于实现负载均衡，通过每个region存储的表的数量来进行
     * @param N 表示要返回的region个数
     * @return  返回N个region
     */
    public List<Integer> findNMinRegion(int N) {
        List<Map.Entry<Integer, List<String>>> list = new ArrayList<Map.Entry<Integer, List<String>>>(regionsToTables.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, List<String>>>() {
            public int compare(Map.Entry<Integer, List<String>> o1, Map.Entry<Integer, List<String>> o2) {
                return (o1.getValue().size() - o2.getValue().size());
            }
        });

        List<Integer> NMinRegion = new ArrayList<>();

        for(int i = 0; i < N; i++) {
            Map.Entry<Integer, List<String>> t = list.get(i);
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
