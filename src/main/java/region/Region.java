package region;

import common.meta.ClientInfo;
import common.meta.ClientInfoFactory;
import common.meta.DMSLog;
import common.meta.table;
import common.rpc.ThriftServer;
import common.zookeeper.Client;
import common.zookeeper.ClientRegionServerImpl;
import config.config;
import lombok.Data;
import master.Master;
import master.MasterImpl;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import region.db.DMSDB;
import region.db.Interpreter;
import region.rpc.Region.Iface;
import region.rpc.execResult;
import region.rpc.Region.Iface;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.*;

import static region.Utils.DBFiles;

public class Region implements Runnable {
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
     */
    private final Logger log = Logger.getLogger(DMSLog.class);
    /**
     * Region自己的Interpreter
     */
    private final Interpreter interpreter;
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
     * 定时触发
     */
    Timer timer = new Timer();

    public Region() throws Exception {
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
        //实例化数据库必要变量
        DMSDB x = new DMSDB(DBFiles + _C.metadata.name + "\\");
        DMSDB.changeDIR(DBFiles + _C.metadata.name + "\\");
        File A = new File(DMSDB.DBDIR.storageSpace);
        if (!A.isDirectory()) A.mkdir();
        //定义独立interpreter
        interpreter = new Interpreter();
    }

    public Region(config _C) throws Exception {
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
        //实例化数据库必要变量
        DMSDB x = new DMSDB(DBFiles + _C.metadata.name + "\\");
        DMSDB.changeDIR(DBFiles + _C.metadata.name + "\\");
        File A = new File(DMSDB.DBDIR.storageSpace);
        if (!A.isDirectory()) A.mkdir();
        //定义独立interpreter
        interpreter = new Interpreter();
    }

    public static void main(String[] args) throws Exception {
        config _CA = new config();
        _CA.loadYaml();

        _CA.network.rpcPort = 2020;
        _CA.network.socketPort = 2021;
        _CA.metadata.name = "Test RegionServer A";

        Region A = new Region(_CA);

        Thread TA = new Thread(A);

        TA.start();
    }

    public void startThriftServer() {
        try {
            region.rpc.Region.Iface handler = new RegionImpl();
            region.rpc.Region.Processor<Iface> processor = new region.rpc.Region.Processor<>(
                    handler);
            ThriftServer server = new ThriftServer(processor, _C.zookeeper.port);
            server.startServer();
            Thread.sleep(1000000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            System.out.println("[Region Server Running] " + _C.metadata.name);
            Thread z = new Thread(() -> {
                try {
                    startThriftServer();
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            z.start();
            Thread t = new Thread(() -> timer.schedule(new TimerTask() {
                public void run() {
                    System.out.println("[Timed Check Log]");
                    boolean temp;
                    for (Map.Entry<String, List<String>> m : regionLog.mainLog.entrySet()) {
                        temp = false;
                        for (table i : tables) {
                            if (Objects.equals(m.getKey(), i.name)) {
                                temp = true;
                                break;
                            }
                        }
                        if (!temp) {
                            DMSDB.changeDIR(DBFiles + _C.metadata.name + "\\");
                            System.out.println("[Flash new Log] " + m.getKey());
                            System.out.println("[All new Log] " + m.getValue());
                            for (String s : m.getValue()) {
                                try {
                                    RI.syncExec(s, m.getKey());
                                } catch (TException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            regionLog.testOutput();
                            MasterImpl MI = new MasterImpl();
                            MI.finishCopyTable(m.getKey(), _C.metadata.uid);
                        }
                    }
                }
            }, 1000, 2000));
            t.start();
            synchronized (this) {
                wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public class RegionImpl implements Iface {
        @Override
        public execResult statementExec(String cmd, String tableName) throws TException {
            DMSDB.changeDIR(DBFiles + _C.metadata.name + "\\");

            execResult res = interpreter.runSingleCommand(cmd);

            if (res.type == 2)
                tables.add(new table(tableName));
            if (res.type == 3)
                tables.remove(new table(tableName));
            if (res.status == 1) {
                System.out.println("[SUCCESS STATE] " + res);
                regionLog.add(tableName, cmd);
            }

            return res;
        }

        @Override
        public boolean requestCopyTable(String destination, String tableName, boolean isMove) throws TException {
            DMSDB.changeDIR(DBFiles + _C.metadata.name + "\\");
            String[] address = destination.split(":");
            boolean result = regionLog.transfer(address[0], address[1], tableName);
            if(isMove)
                regionLog.remove(tableName);
            return result;
        }

        public execResult syncExec(String cmd, String tableName) throws TException {
            DMSDB.changeDIR(DBFiles + _C.metadata.name + "\\");

            execResult res = interpreter.runSingleCommand(cmd);

            if (res.type == 2)
                tables.add(new table(tableName));
            if (res.type == 3)
                tables.remove(new table(tableName));
            if (res.status == 1) {
                System.out.println("[SUCCESS SYNC STATE] " + res);
            }

            return res;
        }

        @Override
        public boolean copyTable(String destination, String tableName) throws IOException, InterruptedException {
            String[] address = destination.split(":");
            copyCommand t = new copyCommand(new Socket(address[0], Integer.parseInt(address[1])), tableName);
            Thread waiter = new Thread(t);
            waiter.start();
            waiter.join();
            return true;
        }

        @Data
        class logLoad implements Serializable {
            public List<String> Log;
            public String tableName;
            public Integer checkPoint;
            public Integer Type;

            public logLoad(List<String> strings, String name, Integer integer, Integer type) {
                Log = strings;
                tableName = name;
                checkPoint = integer;
                Type = type;
            }
        }

        class copyCommand extends Thread {
            private final Socket socket;
            private final logLoad logload;

            public copyCommand(Socket socket, String tableName) {
                this.socket = socket;
                this.logload = new logLoad(null, tableName, null, 1);
            }

            @Override
            public void run() {
                System.out.println("[Send Copy Command] " + socket.toString() + " Table: " + logload.tableName);
                try {
                    ObjectOutputStream sendOut = new ObjectOutputStream(socket.getOutputStream());
                    sendOut.writeObject(logload);
                    sendOut.flush();
                    System.out.println("[End Copy Command] " + logload.tableName);
                } catch (Exception ignored) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

    }
}