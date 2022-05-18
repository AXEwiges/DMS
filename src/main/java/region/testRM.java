package region;

import common.meta.ClientInfo;
import common.rpc.ThriftClient;
import config.Config;
import master.rpc.Master;
import org.apache.thrift.TException;
import region.rpc.execResult;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static region.Utils.DBFiles;

public class testRM {
    static void delFile(File file) {
        if (!file.exists())
            return;

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            assert files != null;
            for (File f : files)
                delFile(f);
        }
        boolean A = file.delete();
    }

    static void clearPath() {
        File DBPATH = new File(DBFiles);
        delFile(DBPATH);
        boolean R = DBPATH.mkdir();
    }

    public static void main(String[] args) {
        RegionFactory regionFactory = new RegionFactory();
        List<Config> configs = regionFactory.regionServerConfigFactory(5);

        clearPath();

        try{
            //方便操作单个Region线程的接口
            List<Thread> regionThreads = new ArrayList<>();

            for(Config C : configs)
                regionThreads.add(new Thread(new Region(C)));

            for(Thread thread : regionThreads)
                thread.start();

            List<String> testTables = new ArrayList<>();

            List<String> tableCreate = new ArrayList<>();

            for(int i = 0;i < 10;i++) {
                testTables.add("Test Table " + (i + 1));
                tableCreate.add("create table " + testTables.get(i) + " (ID int, Name" + i + " char(32), email char(255), primary key(ID));");
            }

            System.out.println("[C P]");

            try {
                System.out.println("[A T]");
                Master.Client master = ThriftClient.getForMaster("127.0.0.1", 9090);
                List<ClientInfo> thisTurn = master.getRegionsOfTable("Test Table 1", true, false);
                System.out.println(thisTurn);
                for(ClientInfo I : thisTurn) {
                    region.rpc.Region.Client region = ThriftClient.getForRegionServer(I.ip, I.rpcPort);
                    System.out.println(tableCreate.get(0));
                    execResult res = region.statementExec(tableCreate.get(0), testTables.get(0));
                    System.out.println(res);
                }
                ClientInfo I = thisTurn.get(0);
                for(int i = 0;i < 5;i++) {
                    if(configs.get(i).network.rpcPort == I.rpcPort){
                        regionThreads.get(i).interrupt();
                    }
                }
            } catch (TException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
