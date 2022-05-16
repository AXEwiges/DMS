package region;

import config.config;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import region.rpc.execResult;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

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
        config _CA = new config();
        _CA.loadYaml();

        _CA.network.rpcPort = 2020;
        _CA.network.socketPort = 2021;
        _CA.metadata.name = "Test RegionServer A";

        config _CB = new config();
        _CB.loadYaml();

        _CB.network.rpcPort = 2022;
        _CB.network.socketPort = 2023;
        _CB.metadata.name = "Test RegionServer B";

        try{
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
        String Root = "E:\\SQL\\DMS\\src\\main\\java\\region\\db\\DBFiles\\";
        File DBPATH = new File(Root);
        delFile(new File(Root));
        boolean R = DBPATH.mkdir();
    }

    @Test
    void statementExecTest() {
        config _CA = new config();
        _CA.loadYaml();

        _CA.network.rpcPort = 2020;
        _CA.network.socketPort = 2021;
        _CA.metadata.name = "Test RegionServer A";

        config _CB = new config();
        _CB.loadYaml();

        _CB.network.rpcPort = 2022;
        _CB.network.socketPort = 2023;
        _CB.metadata.name = "Test RegionServer B";

        clearPath();

        try{
            Region A = new Region(_CA);
            Region B = new Region(_CB);

            Thread TA = new Thread(A);
            Thread TB = new Thread(B);

            TA.start();
            TB.start();

            UUID uuid = UUID.randomUUID();

            System.out.println("[Test UID] " + uuid);

            Map<String, String> testCMD = new LinkedHashMap<String, String>(){{
                put("create table " + "TEST_" + uuid + " (ID int, Name char(32), email char(255), primary key(ID))", "Create table " + "TEST_" + uuid + " successfully\n");
                put("insert into " +"TEST_" + uuid + " values (123, 'CMD', 'TEST@gmail.com')", "Insert successfully\n");
                put("create table " + "TEST_" + uuid + "_1" + " (ID int, Name char(32), email char(255), primary key(ID))", "Create table " + "TEST_" + uuid + "_1" + " successfully\n");
                put("insert into " +"TEST_" + uuid + "_1" + " values (123, 'CMD', 'TEST@gmail.com')", "Insert successfully\n");
            }};

            for(Map.Entry<String, String> statement : testCMD.entrySet()) {
                System.out.println("[Test Statement] " + statement.getKey());
                execResult execRes = B.RI.statementExec(statement.getKey(), "TEST_" + uuid);
                if (Objects.equals(execRes.result, statement.getValue())){
                    System.out.println("[Statement Test Successful] " + execRes);
                }
                else
                    System.out.println("[Statement Test Failed] " + execRes);
            }

            boolean result = B.RI.requestCopyTable("127.0.0.1:" + _CA.network.socketPort, "TEST_" + uuid, true);

            System.out.println("[Transport Result] " + result);

            execResult execRes = A.RI.statementExec("show tables", "TEST_" + uuid);

            System.out.println("[Show Result] " + execRes);

            A.regionLog.testOutput();

            TA.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}