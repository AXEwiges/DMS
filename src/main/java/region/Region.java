package region;

import common.meta.DMSLog;
import common.meta.table;
import config.config;
import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import region.db.Interpreter;
import region.rpc.Region.Iface;
import region.rpc.execResult;
import master.rpc.cacheTable;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Region implements Runnable {
    /**
     * 连接Zookeeper集群
     * */
    private ZooKeeper zk;
    /**
     * 加载服务器配置信息
     * */
    public config _C;
    /**
     * 存储在zookeeper上的region信息
     */
    public cacheTable regionInfo;
    /**
     * 用于储存全部的表名
     * */
    public static List<table> tables = new ArrayList<>();
    /**
     * 用于储存全部的log信息，用于同步
     * */
    public DMSLog regionLog;

    public Region() throws IOException {
        this._C = new config();
        _C.loadYaml();
        _C.network.port = 2188;
        regionLog = new DMSLog(_C);
        regionInfo = new cacheTable(_C.network.ip, _C.network.port, _C.metadata.uid);
    }
    /**
     * Connect to the ZooKeeper server.
     */
    public void connectToZK()
            throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(_C.network.ip + ':' + _C.network.port, _C.network.timeOut, (event) -> {
            System.out.println("Default watcher: " + event);
        });
        // Register this region server
        Stat stat = zk.exists("/region_servers", false);
        if (stat == null) {
            zk.create("/region_servers", null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zk.create("/region_servers/" + _C.metadata.uid, JSON.toJSONString(regionInfo).getBytes(), OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        // Read master
        // TODO
    }

    public void run() {
        try {
            System.out.println("Running");
            for(Map.Entry<String, List<String>> m : regionLog.mainLog.entrySet()){
                for(String s : m.getValue()){
                    System.out.println(s);
                }
            }
            RegionImpl impl = new RegionImpl();
            try{
                impl.requestCopyTable("0.0.0.0:2181", "DMS", false);
            } catch (Exception ignored) {}

            synchronized (this) {
                wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public class RegionImpl implements Iface {
        @Override
        public execResult statementExec(String cmd) throws TException {
            execResult res = Interpreter.runSingleCommand(cmd);
            if(res.type == 2) {
                tables.add(new table(res.result.split(" ")[1]));
            }
            if(res.type == 3) {
                tables.remove(new table(res.result.split(" ")[1]));
            }
            return res;
        }

        @Override
        public boolean requestCopyTable(String destination, String tableName, boolean isMove) throws TException {
            String[] address = destination.split(":");
            regionLog.transfer(address[0], address[1], tableName);
            return false;
        }
        @Override
        public void copyTable() throws TException {

        }
    }

    public static void main(String[] args)
            throws InterruptedException, IOException, KeeperException {
        System.out.println("Input the name of the region server: ");
        Scanner scanner = new Scanner(System.in);
        String name = scanner.nextLine();
        Region rs = new Region();
        rs.regionLog.add("DMS", "insert 3");
        System.out.println("Put");
//        rs.connectToZK();
        rs.run();
    }
}