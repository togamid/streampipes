/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.rest.impl.datalake;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.squareup.moshi.Json;
import okhttp3.OkHttpClient;
import org.apache.commons.io.FileUtils;
import org.apache.streampipes.rest.impl.Couchdb;
import org.apache.streampipes.storage.couchdb.utils.Utils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.apache.streampipes.config.backend.BackendConfig;
import org.apache.streampipes.model.datalake.DataLakeMeasure;
import org.apache.streampipes.rest.impl.datalake.model.DataResult;
import org.apache.streampipes.rest.impl.datalake.model.GroupedDataResult;
import org.apache.streampipes.rest.impl.datalake.model.PageResult;
import org.apache.streampipes.storage.management.StorageDispatcher;
import org.lightcouch.CouchDbClient;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.validation.constraints.Null;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

public class DataLakeManagementV3 {

  private static final double NUM_OF_AUTO_AGGREGATION_VALUES = 2000;
  private SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");


  public List<DataLakeMeasure> getInfos() {
    List<DataLakeMeasure> indices = StorageDispatcher.INSTANCE.getNoSqlStore().getDataLakeStorage()
            .getAllDataLakeMeasures();
    return indices;
  }

  public DataResult getEvents(String index, long startDate, long endDate, String aggregationUnit, int aggregationValue, @Nullable String rp) {
    InfluxDB influxDB = getInfluxDBClient();

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT mean(*) FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " WHERE time > " + startDate * 1000000 + " AND time < " + endDate * 1000000
            + " GROUP BY time(" + aggregationValue + aggregationUnit + ") fill(none) ORDER BY time",
            BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);

    DataResult dataResult = convertResult(result);
    influxDB.close();

    return dataResult;
  }


  public GroupedDataResult getEvents(String index, long startDate, long endDate, String aggregationUnit, int aggregationValue,
                                     String groupingTag, @Nullable String rp) {
    InfluxDB influxDB = getInfluxDBClient();

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT mean(*), count(*) FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " WHERE time > " + startDate * 1000000 + " AND time < " + endDate * 1000000
            + " GROUP BY " + groupingTag + ",time(" + aggregationValue + aggregationUnit + ") fill(none) ORDER BY time",
            BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);

    GroupedDataResult groupedDataResult = convertMultiResult(result);
    influxDB.close();

    return groupedDataResult;
  }

  public DataResult getEvents(String index, long startDate, long endDate, @Nullable String rp) {
    InfluxDB influxDB = getInfluxDBClient();

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT * FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " WHERE time > " + startDate * 1000000 + " AND time < " + endDate * 1000000
            + " ORDER BY time",
            BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);

    DataResult dataResult = convertResult(result);
    influxDB.close();
    return dataResult;
  }

  public GroupedDataResult getEvents(String index, long startDate, long endDate, String groupingTag, @Nullable String rp) {
    InfluxDB influxDB = getInfluxDBClient();

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT * FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " WHERE time > " + startDate * 1000000 + " AND time < " + endDate * 1000000
            + " GROUP BY " + groupingTag
            + " ORDER BY time",
            BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);

    GroupedDataResult groupedDataResult = convertMultiResult(result);
    influxDB.close();

    return groupedDataResult;
  }

  public DataResult getEventsAutoAggregation(String index, long startDate, long endDate, @Nullable String rp)
          throws ParseException {
    InfluxDB influxDB = getInfluxDBClient();
    double numberOfRecords = getNumOfRecordsOfTable(index, influxDB, startDate, endDate, rp);
    influxDB.close();

    if (numberOfRecords == 0) {
      influxDB.close();
      return new DataResult();
    } else if (numberOfRecords <= NUM_OF_AUTO_AGGREGATION_VALUES) {
      influxDB.close();
      return getEvents(index, startDate, endDate, rp);
    } else {
      int aggregatinValue = getAggregationValue(index, influxDB, rp);
      influxDB.close();
      return getEvents(index, startDate, endDate, "ms", aggregatinValue, rp);
    }
  }

  public GroupedDataResult getEventsAutoAggregation(String index, long startDate, long endDate, String groupingTag, @Nullable String rp)
          throws ParseException {
    InfluxDB influxDB = getInfluxDBClient();

    double numberOfRecords = getNumOfRecordsOfTable(index, influxDB, startDate, endDate, rp);
    influxDB.close();

    if (numberOfRecords == 0) {
      influxDB.close();
      return new GroupedDataResult(0, new HashMap<>());
    } else if (numberOfRecords <= NUM_OF_AUTO_AGGREGATION_VALUES) {
      influxDB.close();
      return getEvents(index, startDate, endDate, groupingTag, rp);
    } else {
      int aggregatinValue = getAggregationValue(index, influxDB, rp);
      influxDB.close();
      return getEvents(index, startDate, endDate, "ms", aggregatinValue, groupingTag, rp);
    }
  }


  public DataResult getEventsFromNow(String index, String timeunit, int value,
                                     String aggregationUnit, int aggregationValue, @Nullable String rp)
          throws ParseException {
    InfluxDB influxDB = getInfluxDBClient();

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT mean(*) FROM "
                          + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
                          + " WHERE time > now() -" + value + timeunit
                          + " GROUP BY time(" + aggregationValue + aggregationUnit + ") fill(none) ORDER BY time",
                          BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);

    DataResult dataResult = convertResult(result);

    return dataResult;
  }


  public DataResult getEventsFromNow(String index, String timeunit, int value, @Nullable String rp) {
    InfluxDB influxDB = getInfluxDBClient();

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT * FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " WHERE time > now() -" + value + timeunit + " ORDER BY time",
            BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);

    DataResult dataResult = convertResult(result);
    influxDB.close();

    return dataResult;
  }

  public DataResult getEventsFromNowAutoAggregation(String index, String timeunit, int value, @Nullable String rp)
          throws ParseException {
    InfluxDB influxDB = getInfluxDBClient();
    double numberOfRecords = getNumOfRecordsOfTableFromNow(index, influxDB, timeunit, value, rp);
    if (numberOfRecords == 0) {
      influxDB.close();
      return new DataResult();
    } else if (numberOfRecords <= NUM_OF_AUTO_AGGREGATION_VALUES) {
      influxDB.close();
      return getEventsFromNow(index, timeunit, value, rp);
    } else {
      int aggregationValue = getAggregationValue(index, influxDB, rp);
      influxDB.close();
      return getEventsFromNow(index, timeunit, value, "ms", aggregationValue, rp);
    }
  }


  public PageResult getEvents(String index, int itemsPerPage, int page, @Nullable String rp) {
    InfluxDB influxDB = getInfluxDBClient();

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT * FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " ORDER BY time LIMIT " + itemsPerPage + " OFFSET " + page * itemsPerPage,
            BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);

    DataResult dataResult = convertResult(result);
    influxDB.close();

    int pageSum = getMaxPage(index, itemsPerPage);

    return new PageResult(dataResult.getTotal(), dataResult.getHeaders(), dataResult.getRows(), page, pageSum);
  }

  public void writeMeasurementFromDefaulToCustomRententionPolicy(String index, String rp) {
    InfluxDB influxDB = getInfluxDBClient();
    String rp_default = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    QueryResult result = influxDB.query(new Query("SELECT * INTO " +
            BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index + "_temp"
            + " FROM " + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp_default + "." + index));
    deleteMeasurement(index);

    QueryResult result_2 = influxDB.query(new Query("SELECT * INTO " +
            BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " FROM " + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index + "_temp"));
    deleteMeasurement(index + "_temp");
    influxDB.close();

  }

  public DataResult getRetentionPoliciesOfDatabase() {
    InfluxDB influxDB = getInfluxDBClient();
    Query query = new Query("SHOW RETENTION POLICIES ON " + BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult influx_result = influxDB.query(query);
    DataResult dataResult = convertResult(influx_result);
    influxDB.close();
    return dataResult;
  }


  public boolean createRetentionPolicy(String rp, String duration, String shardDuration, int replication) {
    InfluxDB influxDB = getInfluxDBClient();

    String durationString = "";
    if (duration != null)  {  durationString = " DURATION " + duration; }
    String replicationString = "";
    if (replication != 0) { replicationString = " REPLICATION " + replication; }
    String shardDurationString = "";
    if (shardDuration != null)  { shardDurationString = " SHARD DURATION " + shardDuration; }

    Query query = new Query("CREATE RETENTION POLICY "
            + rp + " ON " + BackendConfig.INSTANCE.getInfluxDatabaseName()
            + durationString
            + replicationString
            + shardDurationString,
            BackendConfig.INSTANCE.getInfluxDatabaseName());

    QueryResult influx_result = influxDB.query(query);
    if (influx_result.hasError() || influx_result.getResults().get(0).getError() != null) {
      influxDB.close();
      return false;
    }
    influxDB.close();
    return true;
  }


  public boolean alterRetentionPolicy(String rp, String duration, String shardDuration, int replication) {
    InfluxDB influxDB = getInfluxDBClient();

    String durationString = "";
    if (duration != null)  {  durationString = " DURATION " + duration; }
    String replicationString = "";
    if (replication != 0) { replicationString = " REPLICATION " + replication; }
    String shardDurationString = "";
    if (shardDuration != null)  { shardDurationString = " SHARD DURATION " + shardDuration; }

    Query query = new Query("ALTER RETENTION POLICY "
            + rp + " ON " + BackendConfig.INSTANCE.getInfluxDatabaseName()
            + durationString
            + replicationString
            + shardDurationString,
            BackendConfig.INSTANCE.getInfluxDatabaseName());

    QueryResult influx_result = influxDB.query(query);
    if (influx_result.hasError() || influx_result.getResults().get(0).getError() != null) {
        influxDB.close();
        return false;

    }
    influxDB.close();
    return true;
  }

  public boolean deleteRetentionPolicy(String rp) {
    InfluxDB influxDB = getInfluxDBClient();
    Query query = new Query("DROP RETENTION POLICY "
            + rp + " ON " + BackendConfig.INSTANCE.getInfluxDatabaseName(),
            BackendConfig.INSTANCE.getInfluxDatabaseName());

    QueryResult influx_result = influxDB.query(query);
    if (influx_result.hasError() || influx_result.getResults().get(0).getError() != null) {
        influxDB.close();
        return false;
    }
    influxDB.close();
    return true;
  }

  public double getNumOfRecordsOfTable(String index, @Nullable  String rp) {
    InfluxDB influxDB = getInfluxDBClient();
    double numOfRecords = 0;

    if (rp == null) {
      rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();
    }

    Query query = new Query("SELECT count(*) FROM "
                      + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index,
                      BackendConfig.INSTANCE.getInfluxDatabaseName());

    QueryResult.Result result = influxDB.query


            (query).getResults().get(0);

    if (result.getSeries() == null) {
      return numOfRecords;
    }

    for (Object item : result.getSeries().get(0).getValues().get(0)) {
      if (item instanceof Double && numOfRecords < Double.parseDouble(item.toString())) {
        numOfRecords = Double.parseDouble(item.toString());
      }
    }
    influxDB.close();
    return numOfRecords;
  }

  public long getStorageSizeOfDatabase() {
    InfluxDB influxDB = getInfluxDBClient();
    Query query = new Query("SHOW STATS for 'shard'");
    QueryResult influx_result = influxDB.query(query);

    long storageSize = 0;
    for (QueryResult.Series series : influx_result.getResults().get(0).getSeries()) {
      storageSize = storageSize + (long) Double.parseDouble(series.getValues().get(0).get(0).toString());
    }
    influxDB.close();
    return storageSize;
  }

  public void deleteMeasurement(String index) {

    InfluxDB influxDB = getInfluxDBClient();
    influxDB.setDatabase(BackendConfig.INSTANCE.getInfluxDatabaseName());
    Query query = new Query("DROP MEASUREMENT " + index);
    QueryResult influx_result = influxDB.query(query);

    if (influx_result.hasError() || influx_result.getResults().get(0).getError() != null) {
      System.out.println(influx_result.getResults().get(0).getError());
      System.out.println("Error!");
      // TODO: Raise error exception
    }
    influxDB.close();

    CouchDbClient couchDbClient = Utils.getCoucbDbClient("data-lake");
    List<JsonObject> couch_result = couchDbClient.view("_all_docs").includeDocs(true).query(JsonObject.class);

    for (JsonObject result : couch_result) {
      if (result.has("measureName")) {
        if (result.get("measureName").toString().equals(index)) {
            String id = result.get("_id").toString();
            String rev = result.get("_rev").toString();
            couchDbClient.remove(result.get("_id").toString(), result.get("_rev").toString());
            break;
        }
      }
    }

    couchDbClient.shutdown();
    // TODO: Raise exception if both not successful
  }

  public PageResult getEvents(String index, int itemsPerPage, @Nullable String rp) throws IOException {
    int page = getMaxPage(index, itemsPerPage);

    if (page > 0) {
      page = page -1;
    }

    return getEvents(index, itemsPerPage, page, rp);
  }

  public StreamingOutput getAllEvents(String index, String outputFormat, String rp) {
    return getAllEvents(index, outputFormat, null, null, rp);
  }

  public StreamingOutput getAllEvents(String index, String outputFormat, @Nullable Long startDate,
                                      @Nullable Long endDate, @Nullable String rp) {
    return new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {
        InfluxDB influxDB = getInfluxDBClient();
        int itemsPerRequest = 10000;

        DataResult dataResult;
        //JSON
        if (outputFormat.equals("json")) {

          Gson gson = new Gson();
          int i = 0;
          boolean isFirstDataObject = true;

          outputStream.write(toBytes("["));
          do {
            Query query = getRawDataQueryWithPage(i, itemsPerRequest, index, startDate, endDate, rp);
            QueryResult result = influxDB.query(query, TimeUnit.MILLISECONDS);
            dataResult = new DataResult();
            if ((result.getResults().get(0).getSeries() != null)) {
              dataResult = convertResult(result.getResults().get(0).getSeries().get(0));
            }

            if (dataResult.getTotal() > 0 ) {
              for (List<Object> row : dataResult.getRows()) {
                if (!isFirstDataObject) {
                  outputStream.write(toBytes(","));
                }

                //produce one json object
                boolean isFirstElementInRow = true;
                outputStream.write(toBytes("{"));
                for (int i1 = 0; i1 < row.size(); i1++) {
                  Object element = row.get(i1);
                  if (!isFirstElementInRow) {
                    outputStream.write(toBytes(","));
                  }
                  isFirstElementInRow = false;
                  if (i1 == 0) {
                    element = ((Double) element).longValue();
                  }
                  //produce json e.g. "name": "Pipes" or "load": 42
                  outputStream.write(toBytes("\"" + dataResult.getHeaders().get(i1) + "\": "
                          + gson.toJson(element)));
                }
                outputStream.write(toBytes("}"));
                isFirstDataObject = false;
              }

              i++;
            }
          } while (dataResult.getTotal() > 0);
          outputStream.write(toBytes("]"));

          //CSV
        } else if (outputFormat.equals("csv")) {
          int i = 0;

          boolean isFirstDataObject = true;

          do {
            Query query = getRawDataQueryWithPage(i, itemsPerRequest, index, startDate, endDate, rp);
            QueryResult result = influxDB.query(query, TimeUnit.MILLISECONDS);
            dataResult = new DataResult();
            if ((result.getResults().get(0).getSeries() != null)) {
              dataResult = convertResult(result.getResults().get(0).getSeries().get(0));
            }

            //Send first header
            if (dataResult.getTotal() > 0) {
              if (isFirstDataObject) {
                boolean isFirst = true;
                for (int i1 = 0; i1 < dataResult.getHeaders().size(); i1++) {
                  if (!isFirst) {
                    outputStream.write(toBytes(";"));
                  }
                  isFirst = false;
                  outputStream.write(toBytes(dataResult.getHeaders().get(i1)));
                }
              }
              outputStream.write(toBytes("\n"));
              isFirstDataObject = false;
            }

            if (dataResult.getTotal() > 0) {
              for (List<Object> row : dataResult.getRows()) {
                boolean isFirstInRow = true;
                for (int i1 = 0; i1 < row.size(); i1++) {
                  Object element = row.get(i1);
                  if (!isFirstInRow) {
                    outputStream.write(toBytes(";"));
                  }
                  isFirstInRow = false;
                  if (i1 == 0) {
                    element = ((Double) element).longValue();
                  }
                  outputStream.write(toBytes(element.toString()));
                }
                outputStream.write(toBytes("\n"));
              }
            }
            i++;
          } while (dataResult.getTotal() > 0);
        }
      }
    };
  }

  public boolean removeAllDataFromDataLake() {
    List<DataLakeMeasure> allMeasurements = getInfos();

    // Remove data from influxdb
    InfluxDB influxDB = getInfluxDBClient();
    for (DataLakeMeasure measure : allMeasurements) {

      Query query = new Query("DROP MEASUREMENT " + measure.getMeasureName(),
              BackendConfig.INSTANCE.getInfluxDatabaseName());

      QueryResult influx_result = influxDB.query(query);

      if (influx_result.hasError() || influx_result.getResults().get(0).getError() != null) {
          influxDB.close();
          return false;
      }
    }

    influxDB.close();



    return true;
  }

  private Query getRawDataQueryWithPage(int page, int itemsPerRequest, String index,
                                        @Nullable Long startDate, @Nullable Long endDate, @Nullable String rp) {
    Query query;

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    if (startDate != null && endDate != null) {
      query = new Query("SELECT * FROM "
              + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
              + " WHERE time > " + startDate * 1000000 + " AND time < " + endDate * 1000000
              + " ORDER BY time LIMIT " + itemsPerRequest
              + " OFFSET " + page * itemsPerRequest,
              BackendConfig.INSTANCE.getInfluxDatabaseName());
    } else {
      query = new Query("SELECT * FROM "
              + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
              + " ORDER BY time LIMIT " + itemsPerRequest
              + " OFFSET " + page * itemsPerRequest,
              BackendConfig.INSTANCE.getInfluxDatabaseName());
    }
    return query;
  }

  private byte[] toBytes(String value) {
    return value.getBytes();
  }

  private int getMaxPage(String index, int itemsPerPage) {
    InfluxDB influxDB = getInfluxDBClient();
    Query query = new Query("SELECT count(*) FROM " + index, BackendConfig.INSTANCE.getInfluxDatabaseName());
    QueryResult result = influxDB.query(query);


    int size = ((Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1)).intValue();
    int page = size / itemsPerPage;

    influxDB.close();
    return page;
  }

  private DataResult convertResult(QueryResult result) {
    List<Map<String, Object>> events = new ArrayList<>();
    if (result.getResults().get(0).getSeries() != null) {
        return convertResult(result.getResults().get(0).getSeries().get(0));
    }
    return new DataResult();
  }


  private DataResult convertResult(QueryResult.Series serie) {

    List<Map<String, Object>> events = new ArrayList<>();
    List<String> columns = serie.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      String replacedColumnName = columns.get(i).replaceAll("mean_", "");
      columns.set(i, replacedColumnName);
    }
    List values = serie.getValues();
    return new DataResult(values.size(), columns, values);
  }

  private GroupedDataResult convertMultiResult(QueryResult result) {
    GroupedDataResult groupedDataResult = new GroupedDataResult();
    if (result.getResults().get(0).getSeries() != null) {
      for (QueryResult.Series series : result.getResults().get(0).getSeries()) {
        String groupName = series.getTags().entrySet().toArray()[0].toString();
        DataResult dataResult = convertResult(series);
        groupedDataResult.addDataResult(groupName, dataResult);
      }
    }
    return groupedDataResult;

  }

  private int getAggregationValue(String index, InfluxDB influxDB, @Nullable String rp) throws ParseException {
    long timerange = getDateFromNewestRecordReOfTable(index, influxDB, rp) - getDateFromOldestRecordReOfTable(index, influxDB, rp);
    double v = timerange / NUM_OF_AUTO_AGGREGATION_VALUES;
    return Double.valueOf(v).intValue();
  }

  private long getDateFromNewestRecordReOfTable(String index, InfluxDB influxDB, @Nullable String rp) throws ParseException {

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    Query query = new Query("SELECT * FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " ORDER BY desc LIMIT 1 ",
            BackendConfig.INSTANCE.getInfluxDatabaseName());

   return getDateFromRecordOfTable(query, influxDB);

  }

  private long getDateFromOldestRecordReOfTable(String index, InfluxDB influxDB, @Nullable String rp) throws ParseException {
    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();
    Query query = new Query("SELECT * FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index
            + " ORDER BY asc LIMIT 1 ",
            BackendConfig.INSTANCE.getInfluxDatabaseName());

    return getDateFromRecordOfTable(query, influxDB);

  }

  private long getDateFromRecordOfTable(Query query, InfluxDB influxDB) throws ParseException {
    QueryResult result = influxDB.query(query);
    int timestampIndex = result.getResults().get(0).getSeries().get(0).getColumns().indexOf("time");
    String stringDate = result.getResults().get(0).getSeries().get(0).getValues().get(0).get(timestampIndex).toString();
    Date date = tryParseDate(stringDate);
    return date.getTime();
  }

  private double getNumOfRecordsOfTable(String index, InfluxDB influxDB, long startDate, long endDate, @Nullable String rp) {
    double numOfRecords = 0;

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    QueryResult.Result result = influxDB.query(new Query("SELECT count(*) FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index +
            " WHERE time > " + startDate * 1000000 + " AND time < " + endDate * 1000000,
            BackendConfig.INSTANCE.getInfluxDatabaseName())).getResults().get(0);

    if (result.getSeries() == null) {
      return numOfRecords;
    }

    for (Object item : result.getSeries().get(0).getValues().get(0)) {
      if (item instanceof Double && numOfRecords < Double.parseDouble(item.toString())) {
        numOfRecords = Double.parseDouble(item.toString());
      }
    }

    return numOfRecords;
  }

  private double getNumOfRecordsOfTableFromNow(String index, InfluxDB influxDB, String timeunit, int value, @Nullable String rp) {
    double numOfRecords = 0;

    if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

    QueryResult.Result result = influxDB.query(new Query("SELECT count(*) FROM "
            + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index +
            " WHERE time > now() -" + value + timeunit,
            BackendConfig.INSTANCE.getInfluxDatabaseName())).getResults().get(0);
    if (result.getSeries() == null) {
      return numOfRecords;
    }

    for (Object item : result.getSeries().get(0).getValues().get(0)) {
      if (item instanceof Double) {
        numOfRecords = Double.parseDouble(item.toString());
      }
    }
    return numOfRecords;
  }


  private InfluxDB getInfluxDBClient() {
    OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder()
            .connectTimeout(120, TimeUnit.SECONDS)
            .readTimeout(120, TimeUnit.SECONDS)
            .writeTimeout(120, TimeUnit.SECONDS);

    return InfluxDBFactory.connect(BackendConfig.INSTANCE.getInfluxUrl(), okHttpClientBuilder);
  }

  private Date tryParseDate(String v) throws ParseException {
    try {
      return dateFormat1.parse(v);
    } catch (ParseException e) {
      return dateFormat2.parse(v);
    }
  }


  public byte[] getImage(String fileRoute) throws IOException {
    fileRoute = getImageFileRoute(fileRoute);
    File file = new File(fileRoute);
    return FileUtils.readFileToByteArray(file);
  }


  public String getImageCoco(String fileRoute) throws IOException {
    fileRoute = getImageFileRoute(fileRoute);
    String cocoRoute = getCocoFileRoute(fileRoute);

    File file = new File(cocoRoute);
    if (!file.exists()) {
      return "";
    } else {
      return FileUtils.readFileToString(file, "UTF-8");
    }
  }


  public void saveImageCoco(String fileRoute, String data) throws IOException {
    fileRoute = getImageFileRoute(fileRoute);
    String cocoRoute = getCocoFileRoute(fileRoute);

    File file = new File(cocoRoute);
    file.getParentFile().mkdirs();
    FileUtils.writeStringToFile(file, data, "UTF-8");

  }

  private String getImageFileRoute(String fileRoute) {
    fileRoute = fileRoute.replace("_", "/");
    fileRoute = fileRoute.replace("/png", ".png");
    return fileRoute;
  }

  private String getCocoFileRoute(String imageRoute) {
    String[] splitedRoute = imageRoute.split("/");
    String route = "";
    for (int i = 0; splitedRoute.length - 2  >= i; i++) {
      route += "/" + splitedRoute[i];
    }
    route += "Coco";
    route += "/" + splitedRoute[splitedRoute.length - 1];
    route = route.replace(".png", ".json");
    return route;
  }

  public void updateLabels(String index, String labelColumn, long startdate, long enddate, String label, @Nullable  String rp) {
    DataResult queryResult = getEvents(index, startdate, enddate, rp);
    Map<String, String> headerWithTypes = getHeadersWithTypes(index, rp);
    List<String> headers = queryResult.getHeaders();

    InfluxDB influxDB = getInfluxDBClient();
    influxDB.setDatabase(BackendConfig.INSTANCE.getInfluxDatabaseName());

    for (List<Object> row : queryResult.getRows()) {
      long timestampValue = Math.round((double) row.get(headers.indexOf("timestamp")));

      Point.Builder p = Point.measurement(index).time(timestampValue, TimeUnit.MILLISECONDS);

      for (int i = 1; i < row.size(); i++) {
        String selected_header = headers.get(i);
        if (!selected_header.equals(labelColumn)) {
          if (headerWithTypes.get(selected_header).equals("integer")) {
            p.addField(selected_header, Math.round((double) row.get(i)));
          } else if (headerWithTypes.get(selected_header).equals("string")) {
            p.addField(selected_header, row.get(i).toString());
          }
        } else {
          p.addField(selected_header, label);
        }
      }
      influxDB.write(p.build());
    }
    influxDB.close();
  }

  private Map<String, String> getHeadersWithTypes(String index, @Nullable String rp) {
      InfluxDB influxDB = getInfluxDBClient();

      if (rp == null) rp = BackendConfig.INSTANCE.getDefaultRetentionPolicyName();

      Query query = new Query("SHOW FIELD KEYS FROM "
              + BackendConfig.INSTANCE.getInfluxDatabaseName() + "." + rp + "." + index,
              BackendConfig.INSTANCE.getInfluxDatabaseName());
      QueryResult result = influxDB.query(query);
      influxDB.close();

      Map<String, String> headerTypes = new HashMap<String, String>();
      for (List<Object> element : result.getResults().get(0).getSeries().get(0).getValues()) {
        headerTypes.put(element.get(0).toString(), element.get(1).toString());
      }
      return headerTypes;
    }
}
