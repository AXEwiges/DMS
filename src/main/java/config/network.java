package config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class network{
    public String ip;
    public int timeOut;
    public int rpcPort;
    public int socketPort;
}
