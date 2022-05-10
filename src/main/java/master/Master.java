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

    /**
     * 开启thriftserver
     * @throws TTransportException
     * @throws InterruptedException
     */
    public void startThriftServer()
            throws TTransportException, InterruptedException {
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
     * 该函数用于实现负载均衡，通过每个region存储的表的数量来进行
     * @param N 表示要返回的region个数
     * @return  返回N个region
     */
    public static List<Integer> findNMinRegion(int N, String tableName) {
        List<Map.Entry<Integer, List<String>>> list = new ArrayList<Map.Entry<Integer, List<String>>>(regionsToTables.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer, List<String>>>() {
            public int compare(Map.Entry<Integer, List<String>> o1, Map.Entry<Integer, List<String>> o2) {
                return (o1.getValue().size() - o2.getValue().size());
            }
        });

        List<Integer> NMinRegion = new ArrayList<>();

        for(int i = 0, j = 0; i < regionsInfomation.size()&&j<N; i++) {
            Map.Entry<Integer, List<String>> t = list.get(i);
            int uid = t.getKey();
            List<Integer> regions = new ArrayList<>();
            /**
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
                System.out.println(clientInfo + " connected, uid = " + clientInfo.uid);
                if(!regionsInfomation.containsKey(clientInfo.uid)) {
                    regionsInfomation.put(clientInfo.uid, clientInfo);
                    regionsToTables.put(clientInfo.uid, new ArrayList<>());
                    //考虑负载均衡给新来的region分配几个表
                    int numOfTables = tablesToRegions.size(); //表的数量
                    int numOfRegions = regionsInfomation.size(); //region的数量
                    int avg = numOfTables * 3 /numOfRegions;
                    int remain = numOfTables*3-avg*numOfRegions;

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
    }
}
