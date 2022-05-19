package config;

import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;
import lombok.Data;

@Data
public class config {
    public String appVersion;
    public metadata metadata;
    public cluster cluster;
    public network network;
    public zookeeper zookeeper;

    public void loadYaml(){
        Yaml yaml = new Yaml();
        InputStream in = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config/server.yaml");
        Map<String, Object> obj = yaml.load(in);
        Map<String, Object> metadata = (Map<String, Object>) obj.get("metadata");
        Map<String, Object> cluster = (Map<String, Object>) obj.get("cluster");
        Map<String, Object> network = (Map<String, Object>) obj.get("network");
        Map<String, Object> zookeeper = (Map<String, Object>) obj.get("zookeeper");

        this.appVersion = (String) obj.get("appVersion");
        this.metadata = new metadata((boolean)metadata.get("isMaster"), (int)metadata.get("uid"), (String)metadata.get("name"));
        this.cluster = new cluster((String)cluster.get("mainTable"), (int)cluster.get("maxTables"), (int)cluster.get("neighbor"));
        this.network = new network((String)network.get("ip"), (int)network.get("timeOut"), (int)network.get("rpcPort"), (int)network.get("socketPort"));
        this.zookeeper = new zookeeper((String)zookeeper.get("ip"), (int)zookeeper.get("port"));
    }

    public static void main(String[] args) {
        config a = new config();
        a.loadYaml();
        System.out.println(a);
    }
}
