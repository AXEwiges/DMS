package common.zookeeper;

public interface ClientConnectionStrategy {
  void onConnect(ClientInfo clientInfo);
  void onDisconnect(ClientInfo clientInfo);
}
