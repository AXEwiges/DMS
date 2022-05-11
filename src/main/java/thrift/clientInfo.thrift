namespace java common.meta

/*用于储存单个的region信息*/
struct ClientInfo {
    1:string ip,
    2:i32 rpcPort,
    3:i32 socketPort,
    4:i32 uid
}
