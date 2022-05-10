package master;

import java.io.IOException;
import java.util.*;

import common.rpc.ThriftClient;
import common.rpc.ThriftServer;
import common.zookeeper.Client;
import common.zookeeper.ClientConnectionStrategy;
import common.zookeeper.ClientInfo;
import common.zookeeper.ClientMasterImpl;
import lombok.Data;
import master.rpc.cacheTable;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import master.rpc.Master.Iface;
import region.rpc.Region;

@Data
public class Master {

    static HashMap<String, List<Integer>> tablesToRegions = new HashMap<>();//存储每张表存放在哪些region上
    static HashMap<Integer, List<String>> regionsToTables = new HashMap<>();//存储每个region已经存放表
    static HashMap<Integer, cacheTable> regionsInfomation = new HashMap<>();//存储每个region的信息，key是region的uid
    static HashMap<Integer, Integer> timesOfVisit = new HashMap<>();//存储每个region被访问的次数，用于检测region繁忙

    /**
     * 开启thriftserver
     */
    public void startThriftServer() {
        try {
            Iface handler = new MasterImpl();
            master.rpc.Master.Processor<Iface> processor = new master.rpc.Master.Processor<>(handler);
            ThriftServer server = new ThriftServer(processor, 5099);
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
            List<Map.Entry<Integer, Integer>> list = new ArrayList<>(timesOfVisit.entrySet());
            list.sort((Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) -> {
                    return (o2.getValue() - o1.getValue()); //将region按照访问次数从大到小排序
                });
            if(list.get(0).getValue()>list.get(1).getValue()*2&&list.get(0).getValue()>100) {//如果某台regionserver访问次数明显过多
                int source_uid = list.get(0).getKey();
                int des_uid = list.get(list.size()-1).getKey();
                List<String> tables = regionsToTables.get(source_uid);
                cacheTable source = regionsInfomation.get(source_uid);
                cacheTable des = regionsInfomation.get(des_uid);
                Region.Client client = ThriftClient.getForRegionServer(source.ip, 5099);
                /*
                 * 把一半的表存储在访问量最少的regionserver上
                 */
                for(int i = 0; i < tables.size()/2; i++) {
                    String tableName = tables.get(0);
                    tables.remove(0);
                    tablesToRegions.get(tableName).remove(source_uid);
                    client.requestCopyTable(des.ip, tableName, true);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 该函数用于实现负载均衡，通过每个region存储的表的数量来进行
     * @param N 表示要返回的region个数
     * @return  返回N个region
     */
    public static List<Integer> findNMinRegion(int N, String tableName) {
        List<Map.Entry<Integer, List<String>>> list = new ArrayList<>(regionsToTables.entrySet());
        list.sort(Comparator.comparingInt((Map.Entry<Integer, List<String>> o) -> o.getValue().size()));

        List<Integer> NMinRegion = new ArrayList<>();

        for(int i = 0, j = 0; i < regionsInfomation.size()&&j<N; i++) {
            Map.Entry<Integer, List<String>> t = list.get(i);
            int uid = t.getKey();
            List<Integer> regions = new ArrayList<>();
            /*
             * 确保返回的regionlist里面不包含已经存有该表的region
             */
            if(tablesToRegions.containsKey(tableName))
                regions = tablesToRegions.get(tableName);
            if(!regions.contains(uid)) {
                NMinRegion.add(t.getKey());
                j++;
            }
        }
        return NMinRegion;
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException, TTransportException {
        Client masterClient = new ClientMasterImpl(new ClientConnectionStrategy() {
            @Override
            public void onConnect(ClientInfo clientInfo) {
                try {
                    System.out.println(clientInfo + " connected, uid = " + clientInfo.uid);
                    if(!regionsInfomation.containsKey(clientInfo.uid)) {
                        regionsInfomation.put(clientInfo.uid, clientInfo);
                        timesOfVisit.put(clientInfo.uid, 0);
                        regionsToTables.put(clientInfo.uid, new ArrayList<>());
                        //考虑负载均衡给新来的region分配几个表
                        int numOfTables = tablesToRegions.size(); //表的数量
                        int numOfRegions = regionsInfomation.size(); //region的数量
                        int avg = numOfTables * 3 /numOfRegions;
                        List<String> tables = regionsToTables.get(clientInfo.uid); //tables存放新建region存有的表list
                        for(Integer uid:regionsToTables.keySet()){
                            String source_ip = regionsInfomation.get(uid).ip;
                            if(tables.size()>=avg) break;
                            try {
                                Region.Client client = ThriftClient.getForRegionServer(source_ip, 5099);
                                if(regionsToTables.get(uid).size()>avg) {
                                    for(int j = 0; j < regionsToTables.get(uid).size()-avg; j++) {
                                        String tableName = regionsToTables.get(uid).get(0);
                                        if(!tables.contains(tableName)) {
                                            regionsToTables.get(uid).remove(0);
                                            tablesToRegions.get(tableName).remove(uid);
                                            client.requestCopyTable(clientInfo.ip, tableName, true);
                                        }
                                        if(tables.size()>=avg) break;
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
                    List<String> list = regionsToTables.get(clientInfo.uid);
                    for(String tableName:list) {
                        tablesToRegions.get(tableName).remove(clientInfo.uid);
                        Integer source_uid = tablesToRegions.get(tableName).get(0);
                        String source_ip = regionsInfomation.get(source_uid).ip;
                        Region.Client client = ThriftClient.getForRegionServer(source_ip, 5099);
                        List<Integer> uids = findNMinRegion(1, tableName);
                        Integer des_uid = uids.get(0);
                        String des_ip = regionsInfomation.get(des_uid).ip;
                        client.requestCopyTable(des_ip, tableName, false);
                    }
                    regionsToTables.remove(clientInfo.uid);
                    timesOfVisit.remove(clientInfo.uid);
                    regionsInfomation.remove(clientInfo.uid);
                } catch (TException e) {
                    e.printStackTrace();
                }
            }
        });

        masterClient.connect("127.0.0.1:2181", new ClientInfo("1.2.3.4", 1), 3000);
        Thread.sleep(1000000);
        Master master = new Master();
        master.startThriftServer();
        Timer timer = new Timer();
        //执行时间，时间单位为毫秒，不得小于等于0
        int cacheTime = 60000;
        //延迟时间，时间单位为毫秒，不得小于等于0
        int delay = 10;
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                Master.reset();
            }
        }, delay, cacheTime);
    }
}
