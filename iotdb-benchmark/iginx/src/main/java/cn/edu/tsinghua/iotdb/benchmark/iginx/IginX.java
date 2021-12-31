package cn.edu.tsinghua.iotdb.benchmark.iginx;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType.TEXT;

public class IginX implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IginX.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String SEPARATOR = "_";

  private final String DB_NAME;
  private final DBConfig dbConfig;
  private Session session;

  private final Map<String, String> normalizeMap;

  public IginX(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    this.DB_NAME = dbConfig.getDB_NAME();
    this.normalizeMap = new HashMap<>();
  }

  @Override
  public void init() throws TsdbException {
    session =
        new Session(
            dbConfig.getHOST().get(0),
            dbConfig.getPORT().get(0),
            dbConfig.getUSERNAME(),
            dbConfig.getPASSWORD());

    try {
      session.openSession();
    } catch (SessionException e) {
      LOGGER.error("Initialize IginX failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      session.executeSql("DELETE FROM *;");
      LOGGER.info("Finish clean data!");
    } catch (SessionException | ExecutionException e) {
      LOGGER.warn("Clear Data failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() throws TsdbException {
    try {
      session.closeSession();
    } catch (SessionException e) {
      LOGGER.error("Failed to close IginX connection because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public boolean registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return true;
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    String group = normalize(batch.getDeviceSchema().getGroup());
    String device = normalize(batch.getDeviceSchema().getDevice());

    StringBuilder builder = new StringBuilder();
    builder
        .append("INSERT INTO ")
        .append(DB_NAME)
        .append(".")
        .append(group)
        .append(".")
        .append(device)
        .append(" ")
        .append("(timestamp");
    batch
        .getDeviceSchema()
        .getSensors()
        .forEach(sensor -> builder.append(", ").append(sensor.getName()));
    builder.append(") VALUES");

    batch
        .getRecords()
        .forEach(
            record -> {
              builder.append("(").append(record.getTimestamp());
              List<Object> values = record.getRecordDataValue();
              for (int i = 0; i < values.size(); i++) {
                if (batch.getDeviceSchema().getSensors().get(i).getSensorType() == TEXT) {
                  builder.append(", ").append("'").append(values.get(i)).append("'");
                } else {
                  builder.append(", ").append(values.get(i));
                }
              }
              builder.append(")");
            });

    String sql = builder.toString();

    try {
      session.executeSql(sql);
      return new Status(true);
    } catch (SessionException | ExecutionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    String sql =
        buildSelectFromClause(preciseQuery.getDeviceSchema(), "")
            + String.format(" WHERE time = %s;", preciseQuery.getTimestamp());
    return executeQuery(sql);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sql =
        buildSelectFromClause(rangeQuery.getDeviceSchema(), "")
            + String.format(
                " WHERE time >= %s AND time <= %s;",
                rangeQuery.getStartTimestamp(), rangeQuery.getEndTimestamp());
    return executeQuery(sql);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    String sql =
        buildSelectFromClause(valueRangeQuery.getDeviceSchema(), "")
            + String.format(
                " WHERE time >= %s AND time <= %s",
                valueRangeQuery.getStartTimestamp(), valueRangeQuery.getEndTimestamp())
            + " AND "
            + buildBooleanExpression(
                valueRangeQuery.getDeviceSchema(), valueRangeQuery.getValueThreshold());
    return executeQuery(sql);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String sql =
        buildSelectFromClause(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun())
            + String.format(
                " WHERE time >= %s AND time <= %s",
                aggRangeQuery.getStartTimestamp(), aggRangeQuery.getEndTimestamp());
    return executeQuery(sql);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    String sql =
        buildSelectFromClause(aggValueQuery.getDeviceSchema(), aggValueQuery.getAggFun())
            + " WHERE "
            + buildBooleanExpression(
                aggValueQuery.getDeviceSchema(), aggValueQuery.getValueThreshold());
    return executeQuery(sql);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    String sql =
        buildSelectFromClause(aggRangeValueQuery.getDeviceSchema(), aggRangeValueQuery.getAggFun())
            + String.format(
                " WHERE time >= %s AND time <= %s",
                aggRangeValueQuery.getStartTimestamp(), aggRangeValueQuery.getEndTimestamp())
            + " AND "
            + buildBooleanExpression(
                aggRangeValueQuery.getDeviceSchema(), aggRangeValueQuery.getValueThreshold());
    return executeQuery(sql);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    String sql =
        buildSelectFromClause(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun())
            + String.format(
                " WHERE time >= %s AND time <= %s",
                groupByQuery.getStartTimestamp(), groupByQuery.getEndTimestamp())
            + String.format("GROUP BY %sms;", groupByQuery.getGranularity());
    return executeQuery(sql);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String sql =
        buildSelectFromClause(latestPointQuery.getDeviceSchema(), "last") + " WHERE time >= 0;";
    return executeQuery(sql);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    String sql =
        buildSelectFromClause(rangeQuery.getDeviceSchema(), "")
            + String.format(
                " WHERE time >= %s AND time <= %s;",
                rangeQuery.getStartTimestamp(), rangeQuery.getEndTimestamp())
            + "ORDER BY TIME DESC;";
    return executeQuery(sql);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    String sql =
        buildSelectFromClause(valueRangeQuery.getDeviceSchema(), "")
            + String.format(
                " WHERE time >= %s AND time <= %s",
                valueRangeQuery.getStartTimestamp(), valueRangeQuery.getEndTimestamp())
            + " AND "
            + buildBooleanExpression(
                valueRangeQuery.getDeviceSchema(), valueRangeQuery.getValueThreshold())
            + "ORDER BY TIME DESC;";
    return executeQuery(sql);
  }

  private Status executeQuery(String sql) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }

    int queryResultPointNum = 0;
    try {
      SessionExecuteSqlResult res = session.executeSql(sql);
      int line = res.getValues().size();
      queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, queryResultPointNum, e, sql);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum, new Exception(t), sql);
    }
  }

  private String buildSelectFromClause(List<DeviceSchema> schemaList, String func) {
    StringBuilder builder = new StringBuilder();
    boolean hasFunc = !func.equals("");
    builder.append("SELECT ");
    schemaList.forEach(
        schema -> {
          String group = normalize(schema.getGroup());
          String device = normalize(schema.getDevice());
          List<Sensor> sensors = schema.getSensors();
          sensors.forEach(
              sensor -> {
                if (hasFunc) {
                  builder
                      .append(func)
                      .append("(")
                      .append(group)
                      .append(".")
                      .append(device)
                      .append(".")
                      .append(sensor.getName())
                      .append(")")
                      .append(",")
                      .append(" ");
                } else {
                  builder
                      .append(group)
                      .append(".")
                      .append(device)
                      .append(".")
                      .append(sensor.getName())
                      .append(",")
                      .append(" ");
                }
              });
        });
    builder.deleteCharAt(builder.length() - 2);
    builder.append("FROM ").append(this.DB_NAME);
    return builder.toString();
  }

  private String buildBooleanExpression(List<DeviceSchema> schemas, double valueThreshold) {
    StringBuilder builder = new StringBuilder();
    for (DeviceSchema schema : schemas) {
      String group = normalize(schema.getGroup());
      String device = normalize(schema.getDevice());

      if (!schema.getSensors().isEmpty()) {
        for (int i = 0; i < schema.getSensors().size(); i++) {
          String sensorName = schema.getSensors().get(i).getName();
          builder
              .append(" AND ")
              .append(group)
              .append(".")
              .append(device)
              .append(".")
              .append(sensorName)
              .append(" > ")
              .append(valueThreshold);
        }
      }
    }
    builder.delete(0, 5);
    return builder.toString();
  }

  private String normalize(String path) {
    String normalizedPath = normalizeMap.get(path);
    if (normalizedPath != null) {
      return normalizedPath;
    }
    String[] parts = path.split(SEPARATOR);
    normalizedPath = String.format("%s_%04d", parts[0], Integer.valueOf(parts[1]));
    normalizeMap.put(path, normalizedPath);
    return normalizedPath;
  }
}
