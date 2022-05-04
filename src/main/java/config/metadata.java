package config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class metadata {
    public boolean isMaster;
    public int uid;
    public String name;
}
