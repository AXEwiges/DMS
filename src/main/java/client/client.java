package client;

import common.meta.ClientInfo;
import common.rpc.ThriftClient;
import master.rpc.Master;
import region.rpc.Region;
import region.rpc.execResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serial;
import java.util.*;

//import 乱七八糟;

class MyExec {
    List<ClientInfo> failed;
    String result;
}

public class client {
    static final int maxBufferSize = 100;
    static LinkedHashMap<String, List<ClientInfo>> buffer = new LinkedHashMap<>() {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<ClientInfo>> eldest) {
            return size() > maxBufferSize;
        }
    };

    public static void main(String[] args) {
        try {
            System.out.println("欢迎来到分布式数据库。");
            System.out.println("请输入SQL指令。支持分行，以\";\"结束。");
            Scanner scan = new Scanner(System.in);
            Master.Client master = ThriftClient.getForMaster("127.0.0.1", 9090);
            StringBuilder rawCmd;
            do {
                rawCmd = new StringBuilder();
                String nLine;
                do {
                    System.out.print("DDB>>");
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
//                    System.out.println(rawCmd);
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
                List<ClientInfo> regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                buffer.put(tableName, regions);
                MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                if (!exec.failed.isEmpty()) {
                    /* 在这里进行错误处理 */
                }
                System.out.println(exec.result);
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
                List<ClientInfo> regions;
                if ((regions = buffer.get(tableName)) == null) {
                    regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                    buffer.put(tableName, regions);
                }
                MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                if (!exec.failed.isEmpty()) {
                    /* 在这里进行错误处理 */
                }
                System.out.println(exec.result);
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
                    List<ClientInfo> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                    System.out.println(exec.result);
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
                    List<ClientInfo> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                    System.out.println(exec.result);
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
                    List<ClientInfo> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                    System.out.println(exec.result);
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
                    List<ClientInfo> regions;
                    if ((regions = buffer.get(tableName)) == null) {
                        regions = master.getRegionsOfTable(tableName, isCreate, isDrop);
                        buffer.put(tableName, regions);
                    }
                    MyExec exec = execute(tableName, regions, isCreate, isDrop, rawCmd);
                    if (!exec.failed.isEmpty()) {
                        /* 在这里进行错误处理 */
                    }
                    System.out.println(exec.result);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* 返回的是处理失败的节点 */
    public static MyExec execute(String tableName, List<ClientInfo> regions, boolean isCreate, boolean isDrop, String rawCmd) {
        MyExec exec = new MyExec();
        try {
            exec.failed = regions;
            Iterator<ClientInfo> iterator = exec.failed.iterator();
            while (iterator.hasNext()) {
                /* 在这里访问Region子节点 */
                ClientInfo cacheItem = iterator.next();
                Region.Client region = ThriftClient.getForRegionServer(cacheItem.ip, cacheItem.rpcPort);
                execResult res = region.statementExec(rawCmd, tableName);
                if (res.status != 0) {
                    iterator.remove();
                    exec.result = res.result;
                    if (isCreate) {
                        buffer.putIfAbsent(tableName, regions);
                    }
                    if (isDrop && buffer.containsKey(tableName)) {
                        buffer.remove(tableName);
                    }
                }
                exec.result = res.result;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exec;
    }
}
