package client;

import common.rpc.ThriftClient;
import master.rpc.Master;
import master.rpc.cacheTable;
import region.rpc.Region;
import region.rpc.execResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serial;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Scanner;

//import 乱七八糟;

class MyExec {
    List<cacheTable> failed;
    String result;
}
public class client {
    static final int maxBufferSize = 100;
    static final int SUCCESS = 1;
    static LinkedHashMap<String, List<cacheTable>> buffer = new LinkedHashMap<>() {
        @Serial
        private static final long serialVersionUID = 1L;
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<cacheTable>> eldest) {
            return size() > maxBufferSize;
        }
    };

    public static void main(String[] args) {
        try {
            Scanner scan = new Scanner(System.in);
            Master.Client master = ThriftClient.getForMaster("1.2.3.4", 1);
            StringBuilder rawCmd;
            do {
                rawCmd = new StringBuilder();
                String nLine;
                do {
                    nLine = scan.nextLine();
                    rawCmd.append(" ").append(nLine);
                } while (nLine.indexOf(';') == -1);
                rawCmd.deleteCharAt(0);
                if (rawCmd.toString().toUpperCase().startsWith("EXECFILE")) {
                    int beginIndex = 8;
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    int endIndex = beginIndex;
                    while (rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != ';') {
                        endIndex++;
                    }
                    String fileName = rawCmd.substring(beginIndex, endIndex);
                    try {
                        BufferedReader br = new BufferedReader(new FileReader(fileName));
                        while (true) {
                            StringBuilder fileCmd = new StringBuilder();
                            String line;
                            do {
                                line = br.readLine();
                                if (line == null) {
                                    br.close();
                                    break;
                                }
                                fileCmd.append(" ").append(nLine);
                            } while (line.indexOf(';') == -1);
                            rawCmd.deleteCharAt(0);
                            handleCmd(fileCmd.toString(), master);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                                    System.out.println(rawCmd);
                    handleCmd(rawCmd.toString(), master);
                }
            } while (!rawCmd.toString().toUpperCase().startsWith("EXIT") && !rawCmd.toString().toUpperCase().startsWith("QUIT"));
            scan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void handleCmd(String rawCmd, Master.Client master) {
        try {
            String CMD = rawCmd.toUpperCase();
            //        if(CMD.startsWith("CREATE TABLE")) {
            if (CMD.matches("^CREATE +TABLE[\\s\\S]*")) {
                int beginIndex = CMD.indexOf("TABLE") + 5;
                while (rawCmd.charAt(beginIndex) == ' ') {
                    beginIndex++;
                }
                int endIndex = beginIndex;
                while (rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != '(') {
                    endIndex++;
                }
                String tableName = rawCmd.substring(beginIndex, endIndex);
                //            System.out.println(tableName);
                boolean isCreate = true;
                boolean isDrop = false;
                List<cacheTable> regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                buffer.put(tableName, regions);
                MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                if (!exec.failed.isEmpty()) {
                    /* 在这里进行错误处理 */
                }
            }
            //        else if(CMD.startsWith("DROP TABLE")) {
            else if (CMD.matches("^DROP +TABLE[\\s\\S]*")) {
                int beginIndex = CMD.indexOf("TABLE") + 5;
                while (rawCmd.charAt(beginIndex) == ' ') {
                    beginIndex++;
                }
                int endIndex = beginIndex;
                while (rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != ';') {
                    endIndex++;
                }
                String tableName = rawCmd.substring(beginIndex, endIndex);
                //            System.out.println(tableName);
                boolean isCreate = false;
                boolean isDrop = true;
                List<cacheTable> regions;
                if ((regions = buffer.get(tableName)) == null) {
                    regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                    buffer.put(tableName, regions);
                }
                MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                if (!exec.failed.isEmpty()) {
                    /* 在这里进行错误处理 */
                }
            } else {
                boolean isCreate = false;
                boolean isDrop = false;
                //            if(CMD.startsWith("CREATE INDEX")) {
                if (CMD.matches("^CREATE +INDEX[\\s\\S]*")) {
                    int beginIndex = CMD.indexOf("INDEX") + 5;
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) != ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) != ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    int endIndex = beginIndex;
                    while (rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != '(') {
                        endIndex++;
                    }
                    String tableName = rawCmd.substring(beginIndex, endIndex);
                    //                System.out.println(tableName);
                    List<cacheTable> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                }
                //            else if(CMD.startsWith("DROP INDEX")) {
                else if (CMD.matches("^DROP +INDEX[\\s\\S]*")) {
                    /* 不知道该怎么处理表名 */
                } else if (CMD.startsWith("SHOW")) {
                    /* 还是不知道怎么处理表名 */
                }
                //            else if(CMD.startsWith("INSERT INTO")) {
                else if (CMD.matches("^INSERT +INTO[\\s\\S]*")) {
                    int beginIndex = CMD.indexOf("INTO") + 4;
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    int endIndex = beginIndex;
                    while (rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != '(') {
                        endIndex++;
                    }
                    String tableName = rawCmd.substring(beginIndex, endIndex);
                    //                System.out.println(tableName);
                    List<cacheTable> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                } else if (CMD.startsWith("SELECT")) {
                    int beginIndex = 6;
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) != ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) != ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    int endIndex = beginIndex;
                    while (rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != ';') {
                        endIndex++;
                    }
                    String tableName = rawCmd.substring(beginIndex, endIndex);
//                                    System.out.println(tableName);
                    List<cacheTable> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                } else if (CMD.startsWith("DELETE")) {
                    int beginIndex = 6;
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) != ' ') {
                        beginIndex++;
                    }
                    while (rawCmd.charAt(beginIndex) == ' ') {
                        beginIndex++;
                    }
                    if (CMD.substring(beginIndex).startsWith("FROM")) {
                        while (rawCmd.charAt(beginIndex) != ' ') {
                            beginIndex++;
                        }
                        while (rawCmd.charAt(beginIndex) == ' ') {
                            beginIndex++;
                        }
                    }
                    int endIndex = beginIndex;
                    while (rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != ';') {
                        endIndex++;
                    }
                    String tableName = rawCmd.substring(beginIndex, endIndex);
                    //                System.out.println(tableName);
                    List<cacheTable> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* 返回的是处理失败的节点 */
    public static MyExec execute(String tableName, List<cacheTable> regions, boolean isCreate, boolean isDrop, String rawCmd) {
        MyExec exec = new MyExec();
        try {
            exec.failed = regions;
            Iterator<cacheTable> iterator = exec.failed.iterator();
            while (iterator.hasNext()) {
                /* 在这里访问Region子节点 */
                cacheTable cacheItem = iterator.next();
                Region.Client region = ThriftClient.getForRegionServer(cacheItem.ip, cacheItem.port);
                execResult res = region.statementExec(rawCmd);
                if (res.status == SUCCESS) {
                    iterator.remove();
                    exec.result = res.result;
                    if (isCreate) {
                        buffer.putIfAbsent(tableName, regions);
                    }
                    if (isDrop && buffer.containsKey(tableName)) {
                        buffer.remove(tableName);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exec;
    }
}
