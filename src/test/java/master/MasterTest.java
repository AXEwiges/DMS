package master;

import common.meta.ClientInfo;
import common.rpc.ThriftClient;
import common.zookeeper.Client;
import config.config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import region.Region;
import region.RegionFactory;
import region.rpc.execResult;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static region.Utils.DBFiles;

class MasterTest {
    @BeforeEach
    void setUp() {
        System.out.println("Start Test");
    }

    @AfterEach
    void tearDown() {
        System.out.println("End Test");
    }

    void delFile(File file) {
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

    void clearPath() {
        File DBPATH = new File(DBFiles);
        delFile(DBPATH);
        boolean R = DBPATH.mkdir();
    }

    @Test
    void testLoadBalancing() {
        RegionFactory regionFactory = new RegionFactory();
        List<config> configs = regionFactory.regionServerConfigFactory(5);

        clearPath();

        try{
            //方便操作单个Region线程的接口
            List<Thread> regionThreads = new ArrayList<>();

            for(config C : configs)
                regionThreads.add(new Thread(new Region(C)));

            for(Thread thread : regionThreads)
                thread.start();

            List<String> testTables = new ArrayList<>();

            List<String> tableCreate = new ArrayList<>();

            for(int i = 0;i < 10;i++) {
                testTables.add("Test Table " + (i + 1));
                tableCreate.add("create table " + testTables.get(i) + " (ID int, Name" + i + " char(32), email char(255), primary key(ID));");
            }

            MasterImpl MI = new MasterImpl();

            List<ClientInfo> thisTurn = MI.getRegionsOfTable("Test Table 1", true, false);

            for(ClientInfo I : thisTurn) {
                region.rpc.Region.Client region = ThriftClient.getForRegionServer(I.ip, I.rpcPort);
                execResult res = region.statementExec(tableCreate.get(0), testTables.get(0));
                System.out.println(res);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}