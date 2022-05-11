//package client;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.io.Serial;
//import java.util.Iterator;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.List;
//import java.util.Scanner;
//
////import 乱七八糟;
//
//class CacheTable {
//    String ip, name;
//    int port, uid;
//
//    public CacheTable(CacheTable cache) {
//        this.ip = cache.ip;
//        this.name = cache.name;
//        this.port = cache.port;
//        this.uid = cache.uid;
//    }
//}
//
//class RegionInfo {
//    List<CacheTable> cache;
//    String tableName;
//
//    public RegionInfo(RegionInfo regions) {
//        this.cache = regions.cache;
//        this.tableName = regions.tableName;
//    }
//}
//
//class ExecResult {
//    int status;
//    String result;
//}
//
//class MyExec {
//    RegionInfo failed;
//    String result;
//}
//public class client {
//    static final int maxBufferSize = 100;
//    static final int SUCCESS = 1;
//    static LinkedHashMap<String, RegionInfo> buffer = new LinkedHashMap<>() {
//        @Serial
//        private static final long serialVersionUID = 1L;
//        @Override
//        protected boolean removeEldestEntry(Map.Entry<String, RegionInfo> eldest) {
//            return size() > maxBufferSize;
//        }
//    };
//
//    public static void main(String[] args) {
//        Scanner scan = new Scanner(System.in);
//        StringBuilder rawCmd;
//        do {
//            rawCmd = new StringBuilder();
//            String nLine;
//            do {
//                nLine = scan.nextLine();
//                rawCmd.append(nLine);
//            } while(nLine.indexOf(';') == -1);
//            if(rawCmd.toString().toUpperCase().startsWith("EXECFILE")) {
//                int beginIndex = 8;
//                while(rawCmd.charAt(beginIndex) == ' ') {
//                    beginIndex++;
//                }
//                int endIndex = beginIndex;
//                while(rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != ';') {
//                    endIndex++;
//                }
//                String fileName = rawCmd.substring(beginIndex, endIndex);
//                try {
//                    BufferedReader br = new BufferedReader(new FileReader(fileName));
//                    while(true) {
//                        StringBuilder fileCmd = new StringBuilder();
//                        String line;
//                        do {
//                            line = br.readLine();
//                            if(line == null) {
//                                br.close();
//                                break;
//                            }
//                            fileCmd.append(nLine);
//                        } while(line.indexOf(';') == -1);
//                        handleCmd(fileCmd.toString());
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//            else {
//                handleCmd(rawCmd.toString());
//            }
//        } while(!rawCmd.toString().toUpperCase().startsWith("EXIT") && !rawCmd.toString().toUpperCase().startsWith("QUIT"));
//        scan.close();
//    }
//
//    public static void handleCmd(String rawCmd) {
//        String CMD = rawCmd.toUpperCase();
//        if(CMD.startsWith("CREATE TABLE")) {
//            int beginIndex = 12;
//            while(rawCmd.charAt(beginIndex) == ' ') {
//                beginIndex++;
//            }
//            int endIndex = beginIndex;
//            while(rawCmd.charAt(endIndex) != ' ') {
//                endIndex++;
//            }
//            String tableName = rawCmd.substring(beginIndex, endIndex);
//            boolean isCreate = true;
//            boolean isDrop = false;
//            RegionInfo regions = getRegionsOfTable(tableName, isCreate, isDrop);
//            buffer.put(tableName, regions);
//            MyExec exec = execute(regions, isCreate, isDrop, rawCmd);
//            if(!exec.failed.cache.isEmpty()) {
//                /* 在这里进行错误处理 */
//            }
//        }
//        else if(CMD.startsWith("DROP TABLE")) {
//            int beginIndex = 10;
//            while(rawCmd.charAt(beginIndex) == ' ') {
//                beginIndex++;
//            }
//            int endIndex = beginIndex;
//            while(rawCmd.charAt(endIndex) != ' ') {
//                endIndex++;
//            }
//            String tableName = rawCmd.substring(beginIndex, endIndex);
//            boolean isCreate = false;
//            boolean isDrop = true;
//            RegionInfo regions;
//            if((regions = buffer.get(tableName)) == null) {
//                regions = getRegionsOfTable(tableName, isCreate, isDrop);
//                buffer.put(tableName, regions);
//            }
//            MyExec exec = execute(regions, isCreate, isDrop, rawCmd);
//            if(!exec.failed.cache.isEmpty()) {
//                /* 在这里进行错误处理 */
//            }
//        }
//        else {
//            boolean isCreate = false;
//            boolean isDrop = false;
//            if(CMD.startsWith("CREATE INDEX")) {
//                int beginIndex = 12;
//                while(rawCmd.charAt(beginIndex) == ' ') {
//                    beginIndex++;
//                }
//                while(rawCmd.charAt(beginIndex) != ' ') {
//                    beginIndex++;
//                }
//                while(rawCmd.charAt(beginIndex) == ' ') {
//                    beginIndex ++;
//                }
//                int endIndex = beginIndex;
//                while(rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != '(') {
//                    endIndex++;
//                }
//                String tableName = rawCmd.substring(beginIndex, endIndex);
//                RegionInfo regions;
//                if((regions = buffer.get(tableName)) == null) {
//                    regions = getRegionsOfTable(tableName, isCreate, isDrop);
//                    buffer.put(tableName, regions);
//                }
//                MyExec exec = execute(regions, isCreate, isDrop, rawCmd);
//                if(!exec.failed.cache.isEmpty()) {
//                    /* 在这里进行错误处理 */
//                }
//            }
//            else if(CMD.startsWith("DROP INDEX")) {
//                /* 不知道该怎么处理表名 */
//            }
//            else if(CMD.startsWith("SHOW")) {
//                /* 还是不知道怎么处理表名 */
//            }
//            else if(CMD.startsWith("INSERT INTO")) {
//                int beginIndex = 11;
//                while(rawCmd.charAt(beginIndex) == ' ') {
//                    beginIndex++;
//                }
//                int endIndex = beginIndex;
//                while(rawCmd.charAt(endIndex) != ' ') {
//                    endIndex++;
//                }
//                String tableName = rawCmd.substring(beginIndex, endIndex);
//                RegionInfo regions;
//                if((regions = buffer.get(tableName)) == null) {
//                    regions = getRegionsOfTable(tableName, isCreate, isDrop);
//                    buffer.put(tableName, regions);
//                }
//                MyExec exec = execute(regions, isCreate, isDrop, rawCmd);
//                if(!exec.failed.cache.isEmpty()) {
//                    /* 在这里进行错误处理 */
//                }
//            }
//            else if(CMD.startsWith("SELECT") || CMD.startsWith("DELETE")) {
//                int beginIndex = CMD.indexOf("FROM") + 4;
//                while(rawCmd.charAt(beginIndex) == ' ') {
//                    beginIndex++;
//                }
//                int endIndex = beginIndex;
//                while(rawCmd.charAt(endIndex) != ' ' && rawCmd.charAt(endIndex) != ';') {
//                    endIndex++;
//                }
//                String tableName = rawCmd.substring(beginIndex, endIndex);
//                RegionInfo regions;
//                if((regions = buffer.get(tableName)) == null) {
//                    regions = getRegionsOfTable(tableName, isCreate, isDrop);
//                    buffer.put(tableName, regions);
//                }
//                MyExec exec = execute(regions, isCreate, isDrop, rawCmd);
//                if(!exec.failed.cache.isEmpty()) {
//                    /* 在这里进行错误处理 */
//                }
//            }
//        }
//    }
//
//    /* 返回的是处理失败的节点 */
//    public static MyExec execute(RegionInfo regions, boolean isCreate, boolean isDrop, String rawCmd) {
//        MyExec exec = new MyExec();
//        exec.failed = new RegionInfo(regions);
//        Iterator<CacheTable> iterator = exec.failed.cache.iterator();
//        while(iterator.hasNext()) {
//            /* 在这里访问Region子节点 */
//            CacheTable cacheTable = iterator.next();
//            ExecResult res = statementExec(cacheTable, rawCmd);
//            if(res.status == SUCCESS) {
//                iterator.remove();
//                exec.result = new String(res.result);
//                if(isCreate) {
//                    buffer.putIfAbsent(regions.tableName, regions);
//                }
//                if(isDrop && buffer.containsKey(regions.tableName)) {
//                    buffer.remove(regions.tableName);
//                }
//            }
//        }
//        return exec;
//    }
//
//}
