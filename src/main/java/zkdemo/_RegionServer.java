package zkdemo;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.Scanner;

import config.config;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class _RegionServer implements Runnable {

    private ZooKeeper zk;
    public config _C;

    public _RegionServer() {
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

    public static void main(String[] args)
            throws InterruptedException, IOException, KeeperException {
        String name;
        System.out.println("Input the name of the region server: ");
        Scanner scanner = new Scanner(System.in);
        name = scanner.nextLine();
        _RegionServer rs = new _RegionServer();
        rs.connectToZK();
        rs.run();
    }
}