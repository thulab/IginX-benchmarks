package com.yahoo.ycsb.db;

import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientFactory;
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import cn.edu.tsinghua.iginx.session_v2.write.Point;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

public class IoTDBClient extends DB {

  private IginXClient client = null;
  private WriteClient writeClient = null;
  private QueryClient queryClient = null;

  private static final Object CACHE_LOCK = new Object();
  private static final String DEFAULT_IOTDB_INFO = "192.168.10.41:2333,192.168.10.43:2333";
  private static List<Point> cachePoints;
  private static String measurement;

  private String db_host = "localhost";
  private int db_port = 2333;
  private static final int cache_threshold = 10000;

  public void init() throws DBException {
    String param;
    if (!getProperties().containsKey("iotdbinfo") || getProperties().getProperty("iotdb") == null) {
      param = DEFAULT_IOTDB_INFO;
      System.err.printf("unable to load iotdbinfo, use %s as default%n", param);
    } else {
      param = getProperties().getProperty("iotdbinfo");
    }
    Set<String> localIps = getLocalIps();
    String[] serversInfo = param.split(",");
    List<String> localServers = new ArrayList<>();
    for (String serverInfo: serversInfo) {
      for(String localIp: localIps) {
        if (serverInfo.contains(localIp)) {
          localServers.add(serverInfo);
          break;
        }
      }
    }
    Random random = new Random(0);
    String[] serverInfo;
    if (localServers.isEmpty()) { // 没有与本机在同一个节点的 iginx
      String server = serversInfo[random.nextInt(serversInfo.length)];
      serverInfo = server.split(":");
    } else {
      String server = localServers.get(random.nextInt(localServers.size()));
      serverInfo = server.split(":");
    }

    if (serverInfo.length != 2) {
      System.err.println("Parse IoTDB Server info failed,it should be ip:port");
    } else {
      this.db_host = serverInfo[0];
      this.db_port = Integer.parseInt(serverInfo[1]);
    }
    IginXException exception = null;
    synchronized (CACHE_LOCK) {
      if (cachePoints == null) {
        cachePoints = new ArrayList<>(cache_threshold);
      }
      try {
        if (client == null) {
          client = IginXClientFactory.create(db_host, db_port);
          writeClient = client.getWriteClient();
          queryClient = client.getQueryClient();
          System.err.printf("create client(%s:%s) succeed%n", db_host, db_port);
        }
      } catch (IginXException e) {
        System.err.printf("create client(%s:%s) failed:%s%n", db_host, db_port, e.toString());
        e.printStackTrace();
        exception = e;
      }
    }
    if (exception != null) {
      throw new DBException(exception);
    }
  }

  public Set<String> getLocalIps() {
    Set<String> ips = new HashSet<>();
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      InetAddress ip;
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        if (networkInterface.isLoopback() || networkInterface.isVirtual() || !networkInterface.isUp()) {
          continue;
        }
        Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
          ip = addresses.nextElement();
          if (ip instanceof Inet4Address) {
            ips.add(ip.getHostAddress());
          }
        }
      }
    } catch (Exception e) {
      System.err.printf("get error when load local ips: %s%n", e.toString());
    }
    return ips;
  }

  public void cleanup() throws DBException {
    try {
      synchronized (CACHE_LOCK) {
        if (!cachePoints.isEmpty()) {
          writeClient.writePoints(cachePoints);
          cachePoints.clear();
        }
      }
      client.close();
    } catch (IginXException e) {
      System.err.printf("cleanup client(%s:%s) failed:%s%n", db_host, db_port, e.toString());
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  // TPCx-IoT will not use this method
  public Status read(String table, String key, Set<String> fields,
                     HashMap<String, ByteIterator> result) {
    return Status.OK;
  }

  // TPCx-IoT will not use this method
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  public Status scan(String table, String filter, String clientFilter, String timestamp,
                     Set<String> fields, long runStartTime, Vector<HashMap<String, ByteIterator>> result1,
                     Vector<HashMap<String, ByteIterator>> result2) {
    //String deviceID = String.format("%s.%s.%s", getSgName(clientFilter), clientFilter, filter);
    String deviceID = String.format("%s.%s", clientFilter, filter);
    long newTimeStamp = Long.parseLong(timestamp);
    long oldTimeStamp;
    Status s1 = null;
    try {
      s1 = scanHelper(deviceID, newTimeStamp, fields, result1);
    } catch (IginXException e) {
      e.printStackTrace();
    }
    if (runStartTime > 0L) {
      long time = newTimeStamp - runStartTime;
      oldTimeStamp = newTimeStamp - time;
    } else {
      oldTimeStamp = newTimeStamp - 1800000L;
    }
    long timestampVal =
        oldTimeStamp + (long) (Math.random() * (newTimeStamp - 10000L - oldTimeStamp));
    Status s2 = null;
    try {
      s2 = scanHelper(deviceID, timestampVal, fields, result2);
    } catch (IginXException e) {
      e.printStackTrace();
    }
    if ((s1 != null && s1.isOk()) && (s2 != null && s2.isOk())) {
      return Status.OK;
    }
    return Status.ERROR;
  }

  private Status scanHelper(String deviceID, long timestamp, Set<String> fields,
                            Vector<HashMap<String, ByteIterator>> result) {
    String path = deviceID + ".field0";
    try {
      IginXTable table = queryClient.query(SimpleQuery.builder().startTime(timestamp)
        .endTime(timestamp + 5000L).addMeasurement(path).build());
      for (IginXRecord record: table.getRecords()) {
        HashMap<String, ByteIterator> rowData = new HashMap<>();
        for (Map.Entry<String, Object> entry: record.getValues().entrySet()) {
          rowData.put("field0", new ByteArrayByteIterator((byte[]) entry.getValue()));
        }
        result.add(rowData);
      }
    } catch (IginXException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  // TPCx-IoT will not use this method
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.OK;
  }

  // only support one sensor now
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    String[] params = key.split(":");
    //String deviceID = String.format("%s.%s.%s", getSgName(params[0]), params[0], params[1]);
    String deviceID = String.format("%s.%s", params[0], params[1]);
    long timestamp = Long.parseLong(params[2]);

    try {
      List<Point> previousCachedPoints = null;
      Map<Long, Map<String, byte[]>> previousCachedData = null;
      if (measurement == null) {
        measurement = values.keySet().iterator().next();
      }
      byte[] binaryValue = values.values().iterator().next().toArray();
      Point point = Point.builder().measurement(deviceID + "." + measurement).timestamp(timestamp)
          .binaryValue(binaryValue).build();
      synchronized (CACHE_LOCK) {
        cachePoints.add(point);
        if (cachePoints.size() >= cache_threshold) {
          previousCachedPoints = cachePoints;
          cachePoints = new ArrayList<>(cache_threshold);
        }
      }
      if (previousCachedPoints != null) {
        writeClient.writePoints(previousCachedPoints);
      }
    } catch (IginXException e) {
      e.printStackTrace();
      System.err
          .printf("write %d points to server failed because %s%n", cache_threshold, e.toString());
      return Status.ERROR;
    }
    return Status.OK;
  }

  // TPCx-IoT will not use this method
  public Status delete(String table, String key) {
    return Status.OK;
  }

}
