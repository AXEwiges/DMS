package region;

import common.meta.ClientInfo;
import common.meta.ClientInfoFactory;
import common.meta.DMSLog;
import common.meta.table;
import common.zookeeper.Client;
import common.zookeeper.ClientRegionServerImpl;
import config.config;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import region.db.Interpreter;
import region.rpc.Region.Iface;
import region.rpc.execResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Region implements Runnable {
    /**
     * 加载服务器配置信息
     */
    public config _C;
    /**
     * 用于储存全部的表名
     */
    public List<table> tables = new ArrayList<>();
    /**
     * 用于储存全部的log信息，用于同步
     */
    public DMSLog regionLog;
    /**
     * 接口实例
     */
    public RegionImpl RI;
    /**
     * 连接Zookeeper集群
     */
    private final Client regionThrift;
    /**
     * 存储在zookeeper的Region的ClientInfo，区分RegionInfo所以暂时如此命名
     */
    private final ClientInfo regionData;
    /**
     * log4j依赖
     * */
    private Logger log = Logger.getLogger(DMSLog.class);

    public Region() throws IOException, InterruptedException, KeeperException {
        //加载配置
        this._C = new config();
        _C.loadYaml();
        //启动接收子线程
        regionLog = new DMSLog(_C);
        //连接zookeeper
        regionThrift = new ClientRegionServerImpl();
        regionData = regionThrift.connect("127.0.0.1:2181",
                ClientInfoFactory.from(_C.zookeeper.ip, _C.network.rpcPort, _C.network.socketPort), _C.network.timeOut);
        //暴露接口
        RI = new RegionImpl();

    }

    public Region(config _C) throws IOException, InterruptedException, KeeperException {
        //加载配置
        this._C = _C;
        //启动接收子线程
        regionLog = new DMSLog(_C);
        //连接zookeeper
        regionThrift = new ClientRegionServerImpl();
        regionData = regionThrift.connect("127.0.0.1:2181",
                ClientInfoFactory.from(_C.zookeeper.ip, _C.network.rpcPort, _C.network.socketPort), _C.network.timeOut);
        //
        RI = new RegionImpl();
        //
        BasicConfigurator.configure();
    }

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        System.out.println("Input the name of the region server: ");
        Scanner scanner = new Scanner(System.in);
        String name = scanner.nextLine();
//        Region rs = new Region();
//        rs.regionLog.add("DMS", "insert 5");
//        System.out.println("Put");
//        rs.run();
    }

    public void run() {
        try {
            log.info("[Region Server Running] " + _C.metadata.name);

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
            if(res.type == 2)
                tables.add(new table(res.result.split(" ")[1]));
            if(res.type == 3)
                tables.remove(new table(res.result.split(" ")[1]));

            return res;
        }

        @Override
        public boolean requestCopyTable(String destination, String tableName, boolean isMove) throws TException {
            String[] address = destination.split(":");
            return regionLog.transfer(address[0], address[1], tableName);
        }
    }
}