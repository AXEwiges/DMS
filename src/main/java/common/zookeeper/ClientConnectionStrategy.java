package common.zookeeper;
import common.meta.ClientInfo;

public interface ClientConnectionStrategy {

  /**
   * 当有 region server 连接 zookeeper 时，会执行此函数。
   * @param clientInfo 新连接的 region server 信息
   */
  void onConnect(ClientInfo clientInfo);

  /**
   * 当有 region server 与 zookeeper 断连时，会执行此函数。
   * @param clientInfo 断连的 region server 信息
   */
  void onDisconnect(ClientInfo clientInfo);
}
