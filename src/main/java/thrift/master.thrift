namespace java master.rpc

/*用于储存单个表的信息*/
struct cacheTable {
    1:string ip,
    2:i32 port,
    4:i32 uid
}
/*用于返回值，包括储存某表的全部RegionServer列表和表名确认信息*/
struct regionInfo {
    1:list<cacheTable> cache,
    2:string tableName
}

service Master {
    /*获取表名，参数分别为: 表的名称，是否建表，是否删表*/
    list<regionInfo> getRegionsOfTable(1:string tableName, 2:bool isCreate, 3:bool isDrop),
    /*存储均衡回调函数，参数分别为: 表的名称，将要加入的新服务器*/
    void finishCopyTable(1:string tableName, 2:i32 uid)
}