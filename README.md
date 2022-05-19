# DMS
浙江大学大规模信息系统构建课程项目，实现一个基于分布式的大规模数据库系统。

## 系统组成

- Master：负责管理 region server 连接，处理表名和 region server 的映射关系。
- Region Server：负责表的存储和 SQL 语句的执行。
- 客户端（Java、Python）：对用户提供访问系统的命令行接口。

外部依赖：

- ZooKeeper：辅助集群连接状态管理。

## 系统部署

请按以下步骤部署并运行本系统。

### 开启 ZooKeeper 服务

前往 [ZooKeeper 官网](https://zookeeper.apache.org/releases.html) 下载 ZooKeeper 3.7.1，然后运行 `bin/zkServer.cmd`。ZooKeeper 默认运行在 2181 端口，记下本机地址和运行端口。

### 运行 Master

1. 本系统使用 Maven 管理 Java 依赖，确保已安装 `pom.xml` 中指示的各项依赖。

2. 修改 `src/main/resources/config/server.yml` 配置文件中下面的字段：

   ```yml
   network:
     ip: 192.168.115.25 # 本机 IP
     timeOut: 3000      # 连接超时（毫秒）
     rpcPort: 9046      # 本机用于处理 Thrift 连接的端口
     socketPort: 9047   # 本机用于处理 Socket 连接的端口
   zookeeper:
     ip: 192.168.115.30 # ZooKeeper 服务器 IP
     port: 2181         # ZooKeeper 服务器端口
   ```

3. 运行 `java.master.Master.main()`。

### 运行 Region  Server

1. 参考上节内容，修改配置文件中的字段。注意，如果是在同一物理机上运行 master 或 region server，端口不能重复。
2. 确保 `src/main/java/region/db/DMSDB.java` 中 `storageSpace` 的值为已存在的路径。
3. 运行 `java.region.Region.main()` 方法。

### 运行 Java 客户端

1. 修改 `src/main/java/client/client.java` 中 `Config` 类下的 `ip` 和 `port` 为 master 机器的地址。
2. 运行 `java.client.client.main()` 方法。

### 运行 Python 客户端

参考 `dms-python-client/README.md`。
