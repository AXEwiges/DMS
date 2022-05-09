package config;

import lombok.AllArgsConstructor;
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

    public void loadYaml(){
        Yaml yaml = new Yaml();
        InputStream in = this.getClass()
                .getClassLoader()
                .getResourceAsStream("config/server.yaml");
        Map<String, Object> obj = yaml.load(in);
        Map<String, Object> metadata = (Map<String, Object>) obj.get("metadata");
        Map<String, Object> cluster = (Map<String, Object>) obj.get("cluster");
        Map<String, Object> network = (Map<String, Object>) obj.get("network");

        this.appVersion = (String) obj.get("appVersion");
        this.metadata = new metadata((boolean)metadata.get("isMaster"), (int)metadata.get("uid"), (String)metadata.get("name"));
        this.cluster = new cluster((String)cluster.get("mainTable"), (int)cluster.get("maxTables"), (int)cluster.get("neighbor"));
        this.network = new network((int)network.get("timeOut"), (String)network.get("ip"), (int)network.get("port"), (int)network.get("recvPort"));
    }

    public static void main(String[] args) {
        config a = new config();
        a.loadYaml();
        System.out.println(a);
    }
}
