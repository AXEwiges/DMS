package common.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class table {
    public static String name;

    public table(){}
    public table(String s) {
        name = s;
    }

}
