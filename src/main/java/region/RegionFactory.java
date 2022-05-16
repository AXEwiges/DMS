package region;

import config.config;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

public class RegionFactory {
    public List<config> regionServerConfigFactory(int num) {
        List<config> configs = new ArrayList<>();
        for(int i = 0;i < num;i++){
            UUID uuid = UUID.randomUUID();
            config _A = new config();
            _A.loadYaml();
            _A.metadata.name = uuid.toString().substring(0, 6);
            _A.network.rpcPort = 2000 + i * 3;
            _A.network.socketPort = 2001 + i * 3;
            _A.zookeeper.port = 2002 + i * 3;
            configs.add(_A);
        }
        return configs;
    }
}
