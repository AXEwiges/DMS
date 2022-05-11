package master;

import master.rpc.Master;
import common.meta.ClientInfo;

import java.nio.Buffer;
import java.util.*;

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
        if(isCreate) {
            regions = master.Master.findNMinRegion(3, tableName);
            master.Master.tablesToRegions.put(tableName, regions);
            for(Integer i:regions) {
                List<String> tables = master.Master.regionsToTables.get(i);
                tables.add(tableName);
            }
        }
        else {
            regions = master.Master.tablesToRegions.get(tableName);
            if(isDrop) {
                for(Integer i:regions) {
                    List<String> tables = master.Master.regionsToTables.get(i);
                    tables.remove(tableName);
                }
            }
        }
        List<ClientInfo> regionsINFO = new ArrayList<>();
        for(Integer i:regions) {
            master.Master.timesOfVisit.replace(i, master.Master.timesOfVisit.get(i) + 1);
            regionsINFO.add(master.Master.regionsInfomation.get(i));
        }
        if(isDrop)
            master.Master.tablesToRegions.remove(tableName);
        return regionsINFO;
    }

    /**
     * 这个函数用于在region完成表的复制操作时被调用
     * @param tableName 表名
     * @param uid 表被复制到的region's uid
     */
    @Override
    public void finishCopyTable(String tableName, int uid) {
        master.Master.tablesToRegions.get(tableName).add(uid);
        master.Master.regionsToTables.get(uid).add(tableName);
    }

    public static void main(String[] args) {
        MasterImpl m = new MasterImpl();
        master.Master.MasterConnectionStrategy masterConnectionStrategy = new master.Master.MasterConnectionStrategy();
        int uid = 1;
        StringBuilder ip = new StringBuilder("1.2.3.4");
        int rpcPort = 1;
        int socketPort = 100;
        for(int i = 0; i < 10; i++) {
            ClientInfo client = new ClientInfo(ip.toString(), rpcPort, socketPort, uid);
            masterConnectionStrategy.onConnect(client);
            uid++;
        }
    }
}
