namespace java region.rpc

/*用于储存语句执行结果   0 错误 1 正常 2 建表，需加入队列 3 删除表，需要移出队列*/
struct execResult {
    1:i32 status,
    2:string result,
    3:i32 type
}

service Region {
    /*执行某个语句，返回成功与否*/
    execResult statementExec(1:string cmd, 2:string tableName),
    /*命令某服务器将某表向另一个服务器拷贝，参数为: 目的地址，表名，是否为迁移/单纯复制*/
    bool requestCopyTable(1:string destination, 2:string tableName, 3:bool isMove),
    /*执行拷贝，备用函数，暂时不移除*/
    bool copyTable(1:string destination, 2:string tableName)
}