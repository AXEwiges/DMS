package region;

import config.config;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RegionFactory {
    public List<config> regionServerConfigFactory(int num) {
        List<config> configs = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            config _A = new config();
            _A.loadYaml();
            _A.metadata.name = "Test Server " + (i + 1);
            _A.network.rpcPort = 2000 + i * 3;
            _A.network.socketPort = 2001 + i * 3;
            configs.add(_A);
        }
        return configs;
    }
}
