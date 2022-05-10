package master;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.*;

import common.zookeeper.Client;
import common.zookeeper.ClientConnectionStrategy;
import common.zookeeper.ClientInfo;
import common.zookeeper.ClientMasterImpl;
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

/**
 * 实现Master接口方法
 */
class MasterImpl implements Iface {

    HashMap<String, List<Integer>> tablesToRegions = new HashMap<>();//存储每张表存放在哪些region上
    HashMap<Integer, List<String>> regionsToTables = new HashMap<>();//存储每个region已经存放表
    HashMap<Integer, cacheTable> regionsInfomation = new HashMap<>();//存储每个region的信息，key是region的uid

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
}

public class Master {
    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException {
        MasterImpl master = new MasterImpl();
        Client masterClient = new ClientMasterImpl(new ClientConnectionStrategy() {
            @Override
            public void onConnect(ClientInfo clientInfo) {
                System.out.println(clientInfo + " connected, uid = " + clientInfo.uid);
                if(!master.regionsInfomation.containsKey(clientInfo.uid)) {
                    master.regionsInfomation.put(clientInfo.uid, clientInfo);
                    master.regionsToTables.put(clientInfo.uid, new ArrayList<>());
                    //这边考虑负载均衡可以给新来的region分配几个表
                }
            }

            @Override
            public void onDisconnect(ClientInfo clientInfo) {
                System.out.println(
                        clientInfo + " disconnected, uid = " + clientInfo.uid);
                List<String> list = master.regionsToTables.get(clientInfo.uid);
                for(String tableName:list) {
                    //TODO 调用region接口 给region发出复制表指令
                    master.tablesToRegions.get(tableName).remove(clientInfo.uid);
                }
                master.regionsToTables.remove(clientInfo.uid);
                master.regionsInfomation.remove(clientInfo.uid);
            }
        });

        masterClient.connect("127.0.0.1:2181", new ClientInfo("1.2.3.4", 1), 3000);
        Thread.sleep(1000000);
    }
}
