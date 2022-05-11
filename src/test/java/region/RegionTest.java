package region;

import config.config;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputFilter;

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
        }
    }
}