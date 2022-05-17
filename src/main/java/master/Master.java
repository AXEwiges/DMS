package master;

import common.meta.ClientInfo;
import common.meta.ClientInfoFactory;
import common.rpc.ThriftClient;
import common.rpc.ThriftServer;
import common.zookeeper.Client;
import common.zookeeper.ClientConnectionStrategy;
import common.zookeeper.ClientMasterImpl;
import config.config;
import lombok.Data;
import master.rpc.Master.Iface;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import region.rpc.Region;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Data
public class Master {
    static ConcurrentHashMap<String, List<Integer>> tablesToRegions = new ConcurrentHashMap<>();//存储每张表存放在哪些region上
    static ConcurrentHashMap<Integer, List<String>> regionsToTables = new ConcurrentHashMap<>();//存储每个region已经存放表
    static ConcurrentHashMap<Integer, ClientInfo> regionsInfomation = new ConcurrentHashMap<>();//存储每个region的信息，key是region的uid
    static ConcurrentHashMap<Integer, Integer> timesOfVisit = new ConcurrentHashMap<>();//存储每个region被访问的次数，用于检测region繁忙
    static boolean isfinish = false;
    static config _C = new config();

    static class MasterConnectionStrategy implements ClientConnectionStrategy {
        @Override
        public void onConnect(ClientInfo clientInfo) {
            try {
                System.out.println(clientInfo + " connected, uid = " + clientInfo.uid);
                if (!regionsInfomation.containsKey(clientInfo.uid)) {
                    regionsInfomation.put(clientInfo.uid, clientInfo);
                    timesOfVisit.put(clientInfo.uid, 0);
                    regionsToTables.put(clientInfo.uid, new CopyOnWriteArrayList<>());
                    //考虑负载均衡给新来的region分配几个表
                    int numOfTables = tablesToRegions.size(); //表的数量
                    int numOfRegions = regionsInfomation.size(); //region的数量
                    int avg = numOfTables * 3 / numOfRegions;
                    List<String> tables = regionsToTables.get(clientInfo.uid); //tables存放新建region存有的表list
                    for (Integer uid : regionsToTables.keySet()) {
                        if (uid == clientInfo.uid) continue;
                        String source_ip = regionsInfomation.get(uid).ip;
                        int source_port = regionsInfomation.get(uid).rpcPort;
                        int tableSize = tables.size();
                        if (tables.size() >= avg) break;
                        try {
                            Region.Client client = ThriftClient.getForRegionServer(source_ip, source_port);
                            if (regionsToTables.get(uid).size() > avg) {
                                int size = regionsToTables.get(uid).size();
                                for (int j = 0, i = 0; i < regionsToTables.get(uid).size() && j < size - avg; ) {
                                    String tableName = regionsToTables.get(uid).get(i);
                                    if (!tables.contains(tableName)) {
                                        regionsToTables.get(uid).remove(i);
                                        tablesToRegions.get(tableName).remove(Integer.valueOf(uid));
                                        client.requestCopyTable(clientInfo.ip + ":" + clientInfo.socketPort, tableName, true);
                                        while(tables.size()==tableSize);
                                        tableSize = tables.size();
                                        j++;
                                    } else {
                                        i++;
                                    }
                                    if (tables.size() >= avg) break;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onDisconnect(ClientInfo clientInfo) {
            try {
                System.out.println(
                        clientInfo + " disconnected, uid = " + clientInfo.uid);
                if (!regionsInfomation.containsKey(clientInfo.uid)) return;
                List<String> list = regionsToTables.get(clientInfo.uid);
                for (String tableName : list) {
                    int i = 0;
                    int source_uid;
                    do {
                        source_uid = tablesToRegions.get(tableName).get(i);
                        i++;
                    } while (source_uid == clientInfo.uid && i < tablesToRegions.get(tableName).size());
                    System.out.println(source_uid);
                    String source_ip = regionsInfomation.get(source_uid).ip;
                    int source_port = regionsInfomation.get(source_uid).rpcPort;
                    Region.Client client = ThriftClient.getForRegionServer(source_ip, source_port);
                    List<Integer> uids = findNMinRegion(1, tablesToRegions.get(tableName));
                    tablesToRegions.get(tableName).remove(Integer.valueOf(clientInfo.uid));
                    if (uids.isEmpty()) continue;
                    Integer des_uid = uids.get(0);
                    String des_ip = regionsInfomation.get(des_uid).ip;
                    int des_port = regionsInfomation.get(des_uid).socketPort;
                    int tableSize = regionsToTables.get(des_uid).size();
                    client.requestCopyTable(des_ip + ":" + des_port, tableName, false);
                    while(regionsToTables.get(des_uid).size()==tableSize);
//                    while (!isfinish) ;
//                    isfinish = false;
                    /*
                    测试finishCopyTable函数
                     */
//                    MasterImpl m = new MasterImpl();
//                    m.finishCopyTable(tableName, des_uid);
                }
                regionsToTables.remove(Integer.valueOf(clientInfo.uid));
                timesOfVisit.remove(Integer.valueOf(clientInfo.uid));
                regionsInfomation.remove(Integer.valueOf(clientInfo.uid));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 开启thriftserver
     */
    public void startThriftServer() {
        try {
            Iface handler = new MasterImpl();
            master.rpc.Master.Processor<Iface> processor = new master.rpc.Master.Processor<>(
                    handler);
            ThriftServer server = new ThriftServer(processor, 9090);
            server.startServer();
            Thread.sleep(1000000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 这个函数用于实现在某台服务器繁忙时，Master进行表迁移，每60秒运行一次
     */
    public static void reset() {
        try {
            if (timesOfVisit.size() >= 3) {
                List<Map.Entry<Integer, Integer>> list = new CopyOnWriteArrayList<>(
                        timesOfVisit.entrySet());
                list.sort(
                        (Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) -> {
                            return (o2.getValue() - o1.getValue()); //将region按照访问次数从大到小排序
                        });
                if (list.get(0).getValue() > list.get(1).getValue() * 2
                        && list.get(0).getValue() > 100) {//如果某台regionserver访问次数明显过多
                    int source_uid = list.get(0).getKey();
                    List<String> tables = regionsToTables.get(source_uid);
                    ClientInfo source = regionsInfomation.get(source_uid);
                    Region.Client client = ThriftClient.getForRegionServer(source.ip,
                            source.rpcPort);
                    /*
                     * 把一半的表存储在其他的regionserver上
                     */
                    for (int i = 0; i < tables.size() / 2; i++) {
                        String tableName = tables.get(0);
                        tables.remove(0);
                        List<Integer> l = findNMinRegion(1, tablesToRegions.get(tableName));
                        int des_uid = l.get(0);
                        ClientInfo des = regionsInfomation.get(des_uid);
                        tablesToRegions.get(tableName).remove(Integer.valueOf(source_uid));
                        int tableSize = regionsToTables.get(des_uid).size();
                        client.requestCopyTable(des.ip + ":" + des.socketPort, tableName, true);
                        while(regionsToTables.get(des_uid).size()==tableSize);
//                        while (!isfinish) ;
//                        isfinish = false;
                        /*
                            测试finishCopyTable函数
                        */
//                        MasterImpl m = new MasterImpl();
//                        m.finishCopyTable(tableName, des_uid);
                    }
                }
            }
            /*
             * 重置访问次数
             */
            for (int uid : timesOfVisit.keySet()) {
                timesOfVisit.replace(uid, 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 该函数用于实现负载均衡，通过每个region存储的表的数量来进行
     *
     * @param N 表示要返回的region个数
     * @return 返回N个region
     */
    public static List<Integer> findNMinRegion(int N, List<Integer> already) {
        List<Map.Entry<Integer, List<String>>> list = new CopyOnWriteArrayList<>(
                regionsToTables.entrySet());
        list.sort(Comparator.comparingInt(
                (Map.Entry<Integer, List<String>> o) -> o.getValue().size()));

        List<Integer> NMinRegion = new CopyOnWriteArrayList<>();

        for (int i = 0, j = 0; i < regionsInfomation.size() && j < N; i++) {
            Map.Entry<Integer, List<String>> t = list.get(i);
            int uid = t.getKey();
            /*
             * 确保返回的regionlist里面不包含已经存有该表的region
             */
            if (!already.contains(uid)) {
                NMinRegion.add(uid);
                j++;
            }
        }
        return NMinRegion;
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException, TTransportException {
        _C.loadYaml();
        Client masterClient = new ClientMasterImpl(new MasterConnectionStrategy());
        masterClient.connect(_C.zookeeper.ip+":"+_C.zookeeper.port, ClientInfoFactory.from(_C.network.ip, _C.network.rpcPort),
                _C.network.timeOut);
        Thread t1 = new Thread(() -> {
            try {
                Master master = new Master();
                master.startThriftServer();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Thread 1 is Interrupted");
            }
        });
        t1.start();
        Thread t2 = new Thread(() -> {
            try {
                Timer timer = new Timer();
                //执行时间，时间单位为毫秒，不得小于等于0
                int cacheTime = 6000;
                //延迟时间，时间单位为毫秒，不得小于等于0
                int delay = 10;
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println("检查是否超载");
                        MasterImpl.print();
                        Master.reset();
                    }
                }, delay, cacheTime);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Thread 1 is Interrupted");
            }
        });
        t2.start();
    }
}
