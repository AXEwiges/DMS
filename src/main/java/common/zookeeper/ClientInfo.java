package common.zookeeper;

import java.util.Objects;

/**
 * 用来向 ZooKeeper 表示 Master 或者 RegionServer 开放 thrift 连接的端口。
 */
public class ClientInfo {

  private final String host;
  private final int port;

  public ClientInfo(String host, int port) {
    this.host = host;
    this.port = port;
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

  public int getPort() {
    return port;
  }

  public String getHost() {
    return host;
  }

  @Override
  public String toString() {
    return host + ":" + port;
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
    return port == that.port && host.equals(that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }
}
