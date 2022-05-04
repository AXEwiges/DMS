namespace java test

struct cacheTable {
    1:string ip
    2:i32 port
    3:string name
    4:i32 uid
    5:string table
}

struct Local {
    1:list<cacheTable> cache,
    2:string masterIP,
    3:i32 masterPort
}

service DMS {
    # connect to zk cluster
    bool clusterConnect(1:string ip, 2:i32 port),
    # get all available tables
    list<string> getAllTable(1:string ip, 2:i32 port),
    # execute SQL statement
    bool statementExec(1:string cmd),
    # get all available RegionServer
    list<cacheTable> getService(1:string table)
}