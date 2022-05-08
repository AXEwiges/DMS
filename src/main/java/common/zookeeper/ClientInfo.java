package common.zookeeper;

import master.rpc.cacheTable;

/**
 * 用来向 ZooKeeper 表示 Master 或者 RegionServer 开放 thrift 连接的端口。
 */
public class ClientInfo extends cacheTable {

  public ClientInfo(String ip, int port) {
    this.ip = ip;
    this.port = port;
  }

  public ClientInfo setUid(int uid) {
    this.uid = uid;
    return this;
  }

  /**
   * 将 hostPort 字符串转换为 ClientInfo 对象
   * @param hostPort 形如 {@code 192.168.1.1:2181} 的字符串
   * @return 对应 ClientInfo 对象
   */
  public static ClientInfo from(String hostPort) {
    String[] arr = hostPort.split(":", 2);
    if (arr.length != 2) {
      throw new IllegalArgumentException(
          hostPort + " does not have the right format.");
    }
    int port;
    try {
      port = Integer.parseInt(arr[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          hostPort + " does not have the right format.");
    }
    return new ClientInfo(arr[0], port);
  }

  @Override
  public String toString() {
    return ip + ":" + port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientInfo that = (ClientInfo) o;
    return uid == that.uid && port == that.port && ip.equals(that.ip);
  }

  @Override
  public int hashCode() {
    return uid;
  }
}
