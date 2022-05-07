package region;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.Scanner;

import config.config;
import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import region.db.Interpreter;
import region.rpc.Region.Iface;
import region.db.Interpreter.*;
import region.rpc.execResult;
import region.tools.DMS_STDOUT;

public class Region implements Runnable {

    private ZooKeeper zk;
    public config _C;

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
            return Interpreter.runSingleCommand(cmd);
        }

        @Override
        public boolean requestCopyTable(String destination, String tableName, boolean isMove) throws TException {
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