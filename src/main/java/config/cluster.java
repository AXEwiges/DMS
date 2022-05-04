package config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class cluster{
    public String mainTable;
    public int maxTables;
    public int neighbor;
}
