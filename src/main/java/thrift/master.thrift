namespace java master.rpc

include "clientInfo.thrift"

service Master {
    /*获取表名，参数分别为: 表的名称，是否建表，是否删表*/
    list<clientInfo.ClientInfo> getRegionsOfTable(1:string tableName, 2:bool isCreate, 3:bool isDrop),
    /*存储均衡回调函数，参数分别为: 表的名称，将要加入的新服务器*/
    void finishCopyTable(1:string tableName, 2:i32 uid)
}