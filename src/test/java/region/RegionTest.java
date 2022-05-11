package region;

import config.config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputFilter;

import static org.junit.jupiter.api.Assertions.*;
import org.apache.log4j.Logger;

class RegionTest {
    Logger log = Logger.getLogger(RegionTest.class);

    @BeforeEach
    void setUp() {
        log.info("Start Test");
    }

    @AfterEach
    void tearDown() {
        log.info("End Test");
    }

    @Test
    void regionCopyTable() throws IOException {
        config _CA = new config();
        _CA.loadYaml();

        _CA.network.rpcPort = 2020;
        _CA.network.socketPort = 2021;
        _CA.metadata.name = "Test RegionServer A";

        config _CB = new config();

        _CB.network.rpcPort = 2022;
        _CB.network.socketPort = 2023;
        _CB.metadata.name = "Test RegionServer B";

        Region A = new Region();
        Region B = new Region();

        B.regionLog.add("T1", "insert into T1(a, b, c) values (5, 6, 7)");
        B.regionLog.add("T1", "insert into T1(a, b, c) values (8, 7, 6)");
        B.regionLog.add("T2", "insert into T2(b, c) values (1, 3)");

        A.run();
        B.run();
    }
}