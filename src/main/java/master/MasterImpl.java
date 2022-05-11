package master;

import master.rpc.Master;
import common.meta.ClientInfo;

import java.util.*;

/**
 * @author: minghao
 * @date: 2022/5/10
 * @description: 实现了Master的接口
 */
public class MasterImpl implements Master.Iface {

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

    @Override
    public void finishCopyTable(String tableName, int uid) {
        master.Master.tablesToRegions.get(tableName).add(uid);
        master.Master.regionsToTables.get(uid).add(tableName);
    }
}
