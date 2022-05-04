package config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class network{
    public int timeOut;
    public String ip;
    public int port;
}
