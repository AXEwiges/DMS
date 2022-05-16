package master;

import common.meta.ClientInfo;
import master.rpc.Master;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author: minghao
 * @date: 2022/5/10
 * @description: 实现了Master的接口
 */
public class MasterImpl implements Master.Iface {

    /*
     * 这个函数用于对于每一个SQL操作，client所真实操作的regionserver所在位置
     * @param tableName 用户操作的表名
     * @param isCreate 是否是创建表操作
     * @param isDrop 是否是删除表操作
     * @return 返回对应的region列表
     */
    @Override
    public List<ClientInfo> getRegionsOfTable(String tableName, boolean isCreate, boolean isDrop) {
        List<Integer> regions;
        if (isCreate) {
            if(master.Master.tablesToRegions.containsKey(tableName)) return new ArrayList<>();
            int n = master.Master.regionsInfomation.size();
            if (n < 3) regions = master.Master.findNMinRegion(n, new ArrayList<>());
            else
                regions = master.Master.findNMinRegion(3, new ArrayList<>());
            master.Master.tablesToRegions.put(tableName, regions);
            for (Integer i : regions) {
                List<String> tables = master.Master.regionsToTables.get(i);
                tables.add(tableName);
            }
        } else {
            if(!master.Master.tablesToRegions.containsKey(tableName)) return new ArrayList<>();
            regions = master.Master.tablesToRegions.get(tableName);
            if (isDrop) {
                for (Integer i : regions) {
                    List<String> tables = master.Master.regionsToTables.get(i);
                    tables.remove(tableName);
                }
            }
        }
        List<ClientInfo> regionsINFO = new ArrayList<>();
        for (Integer i : regions) {
            master.Master.timesOfVisit.replace(i, master.Master.timesOfVisit.get(i) + 1);
            regionsINFO.add(master.Master.regionsInfomation.get(i));
        }
        if (isDrop)
            master.Master.tablesToRegions.remove(tableName);
        System.out.println(regionsINFO);
        return regionsINFO;
    }

    /**
     * 这个函数用于在region完成表的复制操作时被调用
     *
     * @param tableName 表名
     * @param uid       表被复制到的region's uid
     */
    @Override
    public void finishCopyTable(String tableName, int uid) {
        master.Master.tablesToRegions.get(tableName).add(uid);
        master.Master.regionsToTables.get(uid).add(tableName);
    }

    public static void print() {
        System.out.println(master.Master.regionsInfomation.toString());
        System.out.println(master.Master.regionsToTables.toString());
        System.out.println(master.Master.tablesToRegions.toString());
        System.out.println(master.Master.timesOfVisit.toString());
    }

    /**
     * for testing
     *
     * @param args
     */
    public static void main(String[] args) {
        MasterImpl m = new MasterImpl();
        master.Master.MasterConnectionStrategy masterConnectionStrategy = new master.Master.MasterConnectionStrategy();
        int uid = 1;
        int rpcPort = 1;
        int socketPort = 100;
        for (int i = 0; i < 10; i++) {
            String ip = uid + ".2.3.4";
            ClientInfo client = new ClientInfo(ip, rpcPort, socketPort, uid);
            masterConnectionStrategy.onConnect(client);
            uid++;
            rpcPort++;
            socketPort++;
//            print();
        }
        for (int i = 0; i < 20; i++) {
            System.out.println(m.getRegionsOfTable(i + "表", true, false));
//            print();
        }
//        for(int i = 0 ; i < 5; i++) {
//            System.out.println(m.getRegionsOfTable(i+"表", false, true));
////            print();
//        }
//        for(int i = 6 ; i < 10; i++) {
//            System.out.println(m.getRegionsOfTable(i+"表", false, false));
//            print();
//        }
//        print();
//        ClientInfo client = new ClientInfo("1.2.3.4", 1, 100, 1);
//        masterConnectionStrategy.onDisconnect(client);
//        print();
        master.Master.timesOfVisit.replace(1, 101);
        print();
        Timer timer = new Timer();
        //执行时间，时间单位为毫秒，不得小于等于0
        int cacheTime = 6000;
        //延迟时间，时间单位为毫秒，不得小于等于0
        int delay = 10;
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("检查是否超载");
                master.Master.reset();
                print();
            }
        }, delay, cacheTime);
    }
}
