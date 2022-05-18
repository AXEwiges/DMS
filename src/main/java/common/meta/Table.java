package common.meta;

import lombok.Data;

/**
 * @author AXEwiges
 * 用于实例化table对象使用，仅封装String
 * */
@Data
public class Table {
    /**
     * 表名
     * */
    public String name;

    public Table(){}
    public Table(String s) {
        name = s;
    }

}
