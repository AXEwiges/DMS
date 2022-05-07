package region;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Region implements Runnable {

    private ZooKeeper zk;
    public config _C;

    public static List<table> tables = new ArrayList<>();

    public Region() {
        this._C = new config();
        _C.loadYaml();
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
        zk.create("/region_servers/" + _C.metadata.name, _C.metadata.name.getBytes(), OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        // Read master
        // TODO
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

    public static class RegionImpl implements Iface {
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
            String destinationIP = destination.split(":")[0];
            String destinationPort = destination.split(":")[1];

            return false;
        }

        @Override
        public void copyTable() throws TException {

        }
    }

    public static void main(String[] args)
            throws InterruptedException, IOException, KeeperException {
        String name;
        System.out.println("Input the name of the region server: ");
        Scanner scanner = new Scanner(System.in);
        name = scanner.nextLine();
        Region rs = new Region();
        rs.connectToZK();
        rs.run();
    }
}