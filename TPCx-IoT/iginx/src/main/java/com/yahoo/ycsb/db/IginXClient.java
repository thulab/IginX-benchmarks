package com.yahoo.ycsb.db;

import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

public class IginXClient extends DB {

  private Session session = null;

  private static final Object CACHE_LOCK = new Object();
  private static final String DEFAULT_IOTDB_INFO = "172.16.17.25:2333,172.16.17.26:2333,172.16.17.27:2333,172.16.17.28:2333,172.16.17.29:2333";
  private static Map<Long, Map<String, byte[]>> cacheData;
  private static int cacheNum = 0;
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
    SessionException sessionException = null;
    synchronized (CACHE_LOCK) {
      if (cacheData == null) {
        cacheData = new HashMap<>();
      }
      try {
        if (session == null) {
          session = new Session(db_host, db_port);
          session.openSession();
          System.err.printf("start session(%s:%s) succeed%n", db_host, db_port);
        }
      } catch (SessionException e) {
        System.err.printf("start session(%s:%s) failed:%s%n", db_host, db_port, e.toString());
        e.printStackTrace();
        sessionException = e;
      }
    }
    if (sessionException != null) {
      throw new DBException(sessionException);
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
        if (cacheNum > 0) {
          insertRecords(cacheData);
          cacheData.clear();
        }
      }
      session.closeSession();
      session = null;
    } catch (SessionException | ExecutionException e) {
      System.err.printf("cleanup session(%s:%s) failed:%s%n", db_host, db_port, e.toString());
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
    } catch (ExecutionException e) {
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
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    if ((s1 != null && s1.isOk()) && (s2 != null && s2.isOk())) {
      return Status.OK;
    }
    return Status.ERROR;
  }

  private Status scanHelper(String deviceID, long timestamp, Set<String> fields,
                            Vector<HashMap<String, ByteIterator>> result)
      throws ExecutionException {
    long startTime = timestamp;
    long endTime = timestamp + 5000L;
    List<String> paths = Collections.singletonList(deviceID + ".field0");
    try {
      SessionQueryDataSet dataSet;
      dataSet = session.queryData(paths, startTime, endTime);
      for (int i = 0; i < dataSet.getTimestamps().length; i++) {
        HashMap<String, ByteIterator> rowResult = new HashMap<>();
        if (dataSet.getPaths().size() != 0) {
          rowResult
              .put("field0", new ByteArrayByteIterator((byte[]) dataSet.getValues().get(i).get(0)));
          result.add(rowResult);
        }
      }
    } catch (SessionException e) {
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
      Map<Long, Map<String, byte[]>> previousCachedData = null;
      if (measurement == null) {
        measurement = values.keySet().iterator().next();
      }
      byte[] cValue = values.values().iterator().next().toArray();
      synchronized (CACHE_LOCK) {
        cacheData.computeIfAbsent(timestamp, k -> new HashMap<>()).put(deviceID, cValue);
        cacheNum++;
        if (cacheNum >= cache_threshold) {
          previousCachedData = cacheData;
          cacheNum = 0;
          cacheData = new HashMap<>();
        }
      }
      if (previousCachedData != null) {
        insertRecords(previousCachedData);
      }
    } catch (SessionException | ExecutionException e) {
      e.printStackTrace();
      System.err
          .printf("write %d records to server failed because %s%n", cache_threshold, e.toString());
      return Status.ERROR;
    }
    return Status.OK;
  }

//  private void insertRecords(Map<Long, Map<String, byte[]>> cacheData)
//      throws SessionException, ExecutionException {
//    List<String> prefixList = cacheData.values().stream().map(Map::keySet)
//        .flatMap(Collection::stream).distinct().collect(Collectors.toList());
//    List<String> paths = new ArrayList<>();
//    List<DataType> dataTypeList = new ArrayList<>();
//    for (String prefix : prefixList) {
//      paths.add(prefix + "." + measurement);
//      dataTypeList.add(BINARY);
//    }
//    long[] timestamps = new long[cacheData.size()];
//    Object[] valuesList = new Object[cacheData.size()];
//    int index = 0;
//    for (Entry<Long, Map<String, byte[]>> entry : cacheData.entrySet()) {
//      timestamps[index] = entry.getKey();
//      Map<String, byte[]> fieldMap = entry.getValue();
//      Object[] values = new Object[paths.size()];
//      for (int i = 0; i < prefixList.size(); i++) {
//        String prefix = prefixList.get(i);
//        values[i] = fieldMap.get(prefix);
//      }
//      valuesList[index] = values;
//      index++;
//    }
//    session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
//  }

  private void insertRecords(Map<Long, Map<String, byte[]>> cacheData)
      throws SessionException, ExecutionException {
    List<String> prefixList = cacheData.values().stream().map(Map::keySet)
        .flatMap(Collection::stream).distinct().collect(Collectors.toList());
    List<String> paths = new ArrayList<>();
    List<DataType> dataTypeList = new ArrayList<>();
    long[] timestamps = new long[cacheData.size()];
    Map<String, Object[]> valuesMap = new LinkedHashMap<>();
    for (String prefix : prefixList) {
      paths.add(prefix + "." + measurement);
      dataTypeList.add(BINARY);
      valuesMap.put(prefix, new Object[timestamps.length]);
    }
    int index = 0;
    for (Entry<Long, Map<String, byte[]>> entry : cacheData.entrySet()) {
      timestamps[index] = entry.getKey();
      Map<String, byte[]> fieldMap = entry.getValue();
      for (Entry<String, byte[]> e : fieldMap.entrySet()) {
        valuesMap.get(e.getKey())[index] = e.getValue();
      }
      index++;
    }
    Object[] valuesList = new Object[paths.size()];
    int i = 0;
    for (Object values : valuesMap.values()) {
      valuesList[i] = values;
      i++;
    }
    session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
  }

  // TPCx-IoT will not use this method
  public Status delete(String table, String key) {
    return Status.OK;
  }
}
