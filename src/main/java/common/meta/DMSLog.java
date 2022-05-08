package common.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;

/**
 * @author AXEwiges
 * @version 5.8
 * */
@Data
@AllArgsConstructor
@NoArgsConstructor
class logLoad {
    public static List<String> Log;
    public static String tableName;
    public static Integer checkPoint;

    public logLoad(List<String> strings, String name, Integer integer) {
        Log = strings;
        tableName = name;
        checkPoint = integer;
    }
}

/**
 * @author AXEwiges
 * @version 5.8
 * */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DMSLog {
    /**
    * 主要Log，存储全部的语句日志，key为表名，value为日志列表
    * */
    public static HashMap<String, List<String>> mainLog;
    /**
     * 缓存Log，同步中的接收者的临时语句日志，key为表名，value为日志列表
     * */
    public static HashMap<String, List<String>> bufferLog;
    /**
     * 临时Log，用于前述结构销毁和重建中的交换变量，key为表名，value为日志列表
     * */
    public static HashMap<String, List<String>> tempLog;
    /**
     * 各表的传输时刻检查点，用于同步检验
     * */
    public static HashMap<String, Integer> checkPoints;
    /**
     * 用于向日志中某个表项添加一条
     * @param tableName 表名
     * @param statement 语句
     * */
    public static synchronized void add(String tableName, String statement){
        mainLog.get(tableName).add(statement);
        Integer integer = checkPoints.get(tableName) + 1;
        checkPoints.put(tableName, integer);
    }
    /**
     * 用于向目标服务器传输一个表的日志进行同步
     * @param ip ip地址
     * @param port 端口
     * @param tableName 表名
     * */
    public static synchronized void transfer(String ip, String port, String tableName){
        logLoad payload = new logLoad(mainLog.get(tableName), tableName, checkPoints.get(tableName));

    }
    /**
     * 用于接收日志，成功将通知master
     * */
    public static synchronized void receive(){

    }
}
