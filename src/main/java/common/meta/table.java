package common.meta;

import lombok.Data;

/**
 * @author AXEwiges
 * 用于实例化table对象使用，仅封装String
 * */
@Data
public class table {
    /**
     * 表名
     * */
    public String name;

    public table(){}
    public table(String s) {
        name = s;
    }

}
