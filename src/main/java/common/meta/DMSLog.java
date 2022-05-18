package common.meta;

import config.config;
import lombok.Data;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AXEwiges
 * @version 5.8
 * */
@Data
class logLoad implements Serializable {
    /**
     * 传输的日志组成列表
     * */
    public List<String> Log;
    /**
     * 表名信息，用于同步时加入
     * */
    public String tableName;
    /**
     * 传输检查点
     * */
    public Integer checkPoint;
    /**
     * 传输的Type标识符，作为特殊
     * */
    public Integer Type;

    public logLoad(List<String> strings, String name, Integer integer, Integer type) {
        Log = strings;
        tableName = name;
        checkPoint = integer;
        Type = type;
    }
}

public class DMSLog {
    /**
    * 主要Log，存储全部的语句日志，key为表名，value为日志列表
    * */
    public ConcurrentHashMap<String, List<String>> mainLog;
    /**
     * 缓存Log，同步中的接收者的临时语句日志，key为表名，value为日志列表
     * */
    public ConcurrentHashMap<String, List<String>> bufferLog;
    /**
     * 临时Log，用于前述结构销毁和重建中的交换变量，key为表名，value为日志列表
     * */
    public ConcurrentHashMap<String, List<String>> tempLog;
    /**
     * 各表的传输时刻检查点，用于同步检验
     * */
    public ConcurrentHashMap<String, Integer> checkPoints;
    /**
     * 同步线程，不断监听，获取Log
     * */
    public syncRecvThread monitorThread;
    /**
     * 必要的日志config数据
     * */
    public config _LC;
    /**
     * 颜色打印功能的具体对象
     * */
    public TestTools TL;

    public DMSLog(config _C){
        _LC = _C;
        TL = new TestTools();

        mainLog = new ConcurrentHashMap<>();
        bufferLog = new ConcurrentHashMap<>();
        tempLog = new ConcurrentHashMap<>();
        checkPoints = new ConcurrentHashMap<>();
        monitorThread = new syncRecvThread();

        monitorThread.start();
    }
    /**
     * 用于向日志中某个表项添加一条
     * @param tableName 表名
     * @param statement 语句
     * */
    public void add(String tableName, String statement){
        TL.RInfo("Add new statement", _LC.metadata.name, " ", statement);
        if(!mainLog.containsKey(tableName)){
            mainLog.put(tableName, new ArrayList<>());
            checkPoints.put(tableName, 0);
        }
        mainLog.get(tableName).add(statement);
        Integer integer = checkPoints.get(tableName) + 1;
        checkPoints.put(tableName, integer);
    }

    public void stopService() {
        monitorThread.interrupt();
    }

    /**
     * @author AXEwiges
     *
     * 主线程，用于建立socket发送Log用于同步
     * */
    class syncSendThread extends Thread {
        public static logLoad payload;
        private final Socket socket;
        public syncSendThread(logLoad payload, Socket socket) {
            syncSendThread.payload = payload;
            this.socket = socket;
        }

        @Override
        public void run() {
            TL.RInfo(3, "Start SyncData", socket.toString(), "Send Table: ", payload.tableName);
            try{
                ObjectOutputStream sendOut = new ObjectOutputStream(socket.getOutputStream());
                sendOut.writeObject(payload);
                sendOut.flush();
                TL.RInfo(3, "End SyncData", payload.tableName);
            } catch (Exception ignored) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    /**
     * @author AXEwiges
     * 主接受线程，用于监听端口并开启子线程处理新加入的同步Log
     * */
    class syncRecvThread extends Thread {
        public ServerSocket server;
        public syncRecvThread() {
            try {
                this.server = new ServerSocket(_LC.network.socketPort);
                TL.RInfo(2, "Start Thread SyncRecv");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            TL.RInfo(2, "Start Recv Process");
            TL.RInfo(2, "Listening", server.toString());
            while(true){
                Socket recv = null;
                try{
                    recv = server.accept();
                    TL.RInfo(2, "SyncDB command from", String.valueOf(recv.getRemoteSocketAddress()));
                    Thread t = new syncDB(recv);
                    t.start();
                } catch (Exception ignored) {
                    try {
                        if(recv != null)
                            recv.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
    /**
     * @author AXEwiges
     * 子线程，用于监听端口并将接收到的数据写入mainLog
     * */
    class syncDB extends Thread {
        public Socket socket;
        public syncDB(Socket s){
            socket = s;
        }
        @Override
        public void run() {
            TL.RInfo(2, "Start syncDB-------");
            try{
                ObjectInputStream recvInputStream = new ObjectInputStream(socket.getInputStream());
                try{
                    logLoad log = (logLoad)recvInputStream.readObject();
                    synchronized(this){
                        TL.RInfo(2, "Running syncLog-----");
                        for(String statement : log.Log)
                            add(log.tableName, statement);
                        TL.RInfo(2, "Complete syncLog-----");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                TL.RInfo(2, "Complete syncDB------");
            } catch(Exception e){
                e.printStackTrace();
            }

        }
    }

    /**
     * 用于向目标服务器传输一个表的日志进行同步
     * @param ip ip地址
     * @param port 端口
     * @param tableName 表名
     * */
    public synchronized boolean transfer(String ip, String port, String tableName) {
        logLoad payload = new logLoad(mainLog.get(tableName), tableName, checkPoints.get(tableName), 0);
        TL.RInfo(3, "Payload ready, Waiting");
        try {
            syncSendThread sender = new syncSendThread(payload, new Socket(ip, Integer.parseInt(port)));
            Thread waiter = new Thread(sender);
            waiter.start();
            waiter.join();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    /**
     * 用于移动日志时删除原表格
     * @param tableName 需要被删除的表名
     * */
    public synchronized void remove(String tableName){
        mainLog.remove(tableName);
        checkPoints.remove(tableName);
    }
    /**
     * 用于打印所有的表，用于测试
     * */
    public void testOutput(){
        TL.RInfo(4, "Test Output", _LC.metadata.name);
        for(Map.Entry<String, List<String>> m : mainLog.entrySet()){
            TL.RInfo(4, "Table Name", m.getKey());
            TL.RInfo(6, "--------------------");
            for(String s : m.getValue()){
                TL.RInfo(4, "Table Log", s);
            }
            TL.RInfo(6, "--------------------");
        }
    }
}
