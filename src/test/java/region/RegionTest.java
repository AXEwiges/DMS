package region;

import common.meta.TestTools;
import config.Config;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import region.rpc.execResult;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static region.Utils.DBFiles;

class RegionTest {

    @BeforeEach
    void setUp() {
        System.out.println("Start Test");
    }

    @AfterEach
    void tearDown() {
        System.out.println("End Test");
    }

    @Test
    void regionCopyTable() throws IOException {
        Config _CA = new Config();
        _CA.loadYaml();

        _CA.network.rpcPort = 2020;
        _CA.network.socketPort = 2021;
        _CA.metadata.name = "Test RegionServer A";

        Config _CB = new Config();
        _CB.loadYaml();

        _CB.network.rpcPort = 2022;
        _CB.network.socketPort = 2023;
        _CB.metadata.name = "Test RegionServer B";

        try {
            Region A = new Region(_CA);
            Region B = new Region(_CB);

            B.regionLog.add("T1", "insert into T1(a, b, c) values (5, 6, 7)");
            B.regionLog.add("T1", "insert into T1(a, b, c) values (8, 7, 6)");
            B.regionLog.add("T2", "insert into T2(b, c) values (1, 3)");

            new Thread(A).start();
            new Thread(B).start();

            boolean result = B.RI.requestCopyTable("127.0.0.1:" + _CA.network.socketPort, "T1", true);

            System.out.println("[Test Result] " + result);
            A.regionLog.testOutput();

        } catch (InterruptedException | KeeperException | TException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    void statementExecTest() {
        TestTools TL = new TestTools();

        Config _CA = new Config();
        _CA.loadYaml();

        _CA.network.rpcPort = 2020;
        _CA.network.socketPort = 2021;
        _CA.metadata.name = "Test RegionServer A";

        Config _CB = new Config();
        _CB.loadYaml();

        _CB.network.rpcPort = 2022;
        _CB.network.socketPort = 2023;
        _CB.metadata.name = "Test RegionServer B";

        clearPath();

        try {
            Region A = new Region(_CA);
            Region B = new Region(_CB);

            Thread TA = new Thread(A);
            Thread TB = new Thread(B);

            TA.start();
            TB.start();

            UUID uuid = UUID.randomUUID();

            TL.RInfo(6, "Test UID", String.valueOf(uuid));

            Map<String, String> testCMD = new LinkedHashMap<String, String>() {{
                put("create table " + "TEST_" + uuid + " (ID int, Name char(32), email char(255), primary key(ID));", "Create table " + "TEST_" + uuid + " successfully\n");
                put("insert into " + "TEST_" + uuid + " values (123, 'CMD', 'TEST@gmail.com');", "Insert successfully\n");
                put("create table " + "TEST_" + uuid + "_1" + " (ID int, Name char(32), email char(255), primary key(ID));", "Create table " + "TEST_" + uuid + "_1" + " successfully\n");
                put("insert into " + "TEST_" + uuid + "_1" + " values (123, 'CMD', 'TEST@gmail.com');", "Insert successfully\n");
                put("select * from " + "TEST_" + uuid + ";", "");
            }};

            for (Map.Entry<String, String> statement : testCMD.entrySet()) {
                TL.RInfo(6, "Test Statement", statement.getKey());
                execResult execRes = B.RI.statementExec(statement.getKey(), "TEST_" + uuid);
                if (Objects.equals(execRes.result, statement.getValue())) {
                    TL.RInfo(1, "Statement Test Successful", String.valueOf(execRes));
                } else
                    TL.RInfo(0, "Statement Test Failed", String.valueOf(execRes));
            }

            boolean result = B.RI.requestCopyTable("127.0.0.1:" + _CA.network.socketPort, "TEST_" + uuid, true);

            TL.RInfo(4, "Transport Result", String.valueOf(result));

            execResult execRes = A.RI.statementExec("show tables", "TEST_" + uuid + ";");

            TL.RInfo(4, "Show Result", String.valueOf(execRes));

            A.regionLog.testOutput();

            TA.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}