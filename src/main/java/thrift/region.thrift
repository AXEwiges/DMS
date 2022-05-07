namespace java region.rpc

/*用于储存语句执行结果*/
struct execResult {
    1:i32 status,
    2:string result
}

service Region {
    /*执行某个语句，返回成功与否*/
    execResult statementExec(1:string cmd),
    /*命令某服务器将某表向另一个服务器拷贝，参数为: 目的地址，表名，是否为迁移/单纯复制*/
    bool requestCopyTable(1:string destination, 2:string tableName, 3:bool isMove),
    /*接受命令，开始接收log进行数据同步*/
    void copyTable()
}