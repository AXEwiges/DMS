package config;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class zookeeper {
    public String ip;
    public int port;
}
