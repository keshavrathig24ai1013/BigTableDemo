import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class BigTable {
    private final String projectId = "bigtabledemo-463315";
    private final String instanceId = "assignment4";
    private final String COLUMN_FAMILY = "sensor";
    private final String tableId = "weather";

    private BigtableDataClient dataClient;
    private BigtableTableAdminClient adminClient;

    public static void main(String[] args) throws Exception {
        BigTable app = new BigTable();
        app.execute();
    }

    public void execute() throws Exception {
        setupClients();

        deleteTable();
        createTable();
        ingestData();

        System.out.println("Temperature: " + getTemperature());
        System.out.println("WindSpeed: " + getMaxWindSpeed());

        List<Object[]> readings = getSeaTacReadings();
        System.out.println("\n=== SeaTac Readings for Oct 2, 2022 ===");
        displayData(readings);

        System.out.println("Temperature: " + getHighestSummerTemperature());
        calculateAvgHumidity("2022-09-15");

        closeClients();
    }

    private void setupClients() throws IOException {
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                .setProjectId(projectId).setInstanceId(instanceId).build();
        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId).setInstanceId(instanceId).build();
        dataClient = BigtableDataClient.create(dataSettings);
        adminClient = BigtableTableAdminClient.create(adminSettings);
        System.out.println("Connected to instance: " + instanceId);
    }

    private void closeClients() {
        dataClient.close();
        adminClient.close();
    }

    private void createTable() {
        try {
            CreateTableRequest request = CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
            adminClient.createTable(request);
            System.out.println("Created table: " + tableId);
        } catch (Exception e) {
            System.err.println("Create table failed: " + e.getMessage());
        }
    }

    private void ingestData() throws Exception {
        String basePath = "src/bin/data/";
        loadStation(basePath + "seatac.csv", "SEA");
        loadStation(basePath + "vancouver.csv", "YVR");
        loadStation(basePath + "portland.csv", "PDX");
    }

    private void loadStation(String filePath, String stationCode) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        boolean skipHeaders = true;
        Set<String> hourMarkers = new HashSet<>();
        BulkMutation batch = BulkMutation.create(TableId.of(tableId));

        while ((line = reader.readLine()) != null) {
            if (skipHeaders && line.contains("Date,Time")) {
                skipHeaders = false;
                continue;
            }
            if (skipHeaders) continue;

            String[] parts = line.split(",");
            if (parts.length < 9) continue;

            String date = parts[1].trim();
            String time = parts[2].trim();
            String hour = String.format("%02d", Integer.parseInt(time.split(":" )[0]));
            String marker = date + "-" + hour;

            if (!hourMarkers.add(marker)) continue;

            String rowKey = stationCode + "#" + date + "#" + hour;
            Mutation mutation = Mutation.create()
                    .setCell(COLUMN_FAMILY, "temperature", parts[3].trim())
                    .setCell(COLUMN_FAMILY, "dewPoint", parts[4].trim())
                    .setCell(COLUMN_FAMILY, "humidity", parts[5].trim())
                    .setCell(COLUMN_FAMILY, "windSpeed", parts[6].trim())
                    .setCell(COLUMN_FAMILY, "gust", parts[7].trim())
                    .setCell(COLUMN_FAMILY, "pressure", parts[8].trim())
                    .setCell(COLUMN_FAMILY, "time", time);

            batch.add(rowKey, mutation);
        }
        reader.close();
        dataClient.bulkMutateRows(batch);
        System.out.println("Data loaded for: " + stationCode);
    }

    private int getTemperature() {
        Row row = dataClient.readRow(TableId.of(tableId), "YVR#2022-10-01#10");
        if (row != null) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                return Integer.parseInt(cell.getValue().toStringUtf8());
            }
        }
        return 0;
    }

    private int getMaxWindSpeed() {
        int maxSpeed = 0;
        Query query = Query.create(TableId.of(tableId))
                .range(ByteStringRange.create("PDX#2022-09-01", "PDX#2022-09-31"));
        for (Row row : dataClient.readRows(query)) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "windSpeed")) {
                try {
                    int speed = Integer.parseInt(cell.getValue().toStringUtf8());
                    maxSpeed = Math.max(maxSpeed, speed);
                } catch (NumberFormatException ignored) {}
            }
        }
        return maxSpeed;
    }

    private List<Object[]> getSeaTacReadings() {
        List<Object[]> results = new ArrayList<>();
        Query query = Query.create(TableId.of(tableId))
                .range(ByteStringRange.create("SEA#2022-10-02#00", "SEA#2022-10-02#24"));

        for (Row row : dataClient.readRows(query)) {
            String[] key = row.getKey().toStringUtf8().split("#");
            Object[] record = new Object[7];
            record[0] = key[1];
            record[1] = key[2];
            for (RowCell cell : row.getCells()) {
                String qualifier = cell.getQualifier().toStringUtf8();
                String value = cell.getValue().toStringUtf8();
                switch (qualifier) {
                    case "temperature": record[2] = Integer.parseInt(value); break;
                    case "dewPoint": record[3] = Integer.parseInt(value); break;
                    case "humidity": record[4] = value; break;
                    case "windSpeed": record[5] = value; break;
                    case "pressure": record[6] = value; break;
                }
            }
            results.add(record);
        }
        return results;
    }

    private int getHighestSummerTemperature() {
        int maxTemp = -100;
        for (String station : Arrays.asList("PDX", "SEA", "YVR")) {
            maxTemp = Math.max(maxTemp, fetchMaxTemp(Query.create(TableId.of(tableId))
                    .range(ByteStringRange.create(station + "#2022-07-01", station + "#2022-07-32")), maxTemp));
            maxTemp = Math.max(maxTemp, fetchMaxTemp(Query.create(TableId.of(tableId))
                    .range(ByteStringRange.create(station + "#2022-08-01", station + "#2022-08-32")), maxTemp));
        }
        return maxTemp;
    }

    private int fetchMaxTemp(Query query, int currentMax) {
        for (Row row : dataClient.readRows(query)) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                try {
                    currentMax = Math.max(currentMax, Integer.parseInt(cell.getValue().toStringUtf8()));
                } catch (NumberFormatException ignored) {}
            }
        }
        return currentMax;
    }

    private void calculateAvgHumidity(String date) {
        int sum = 0, count = 0;
        for (String station : Arrays.asList("PDX", "SEA", "YVR")) {
            Query query = Query.create(TableId.of(tableId))
                    .range(ByteStringRange.create(station + "#" + date + "#00", station + "#" + date + "#24"));
            for (Row row : dataClient.readRows(query)) {
                for (RowCell cell : row.getCells(COLUMN_FAMILY, "humidity")) {
                    try {
                        sum += Integer.parseInt(cell.getValue().toStringUtf8());
                        count++;
                    } catch (NumberFormatException ignored) {}
                }
            }
        }
        if (count > 0) {
            System.out.println("Average humidity on " + date + ": " + (sum / count) + "%");
        } else {
            System.out.println("No data found for humidity on " + date);
        }
    }

    private void deleteTable() {
        try {
            adminClient.deleteTable(tableId);
            System.out.println("Deleted table: " + tableId);
        } catch (NotFoundException e) {
            System.err.println("Table does not exist: " + e.getMessage());
        }
    }

    private void displayData(List<Object[]> records) {
        String[] headers = {"Date", "Hour", "Temperature", "Dewpoint", "Humidity", "Windspeed", "Pressure"};
        int[] widths = Arrays.stream(headers).mapToInt(String::length).toArray();

        for (Object[] row : records) {
            for (int i = 0; i < row.length; i++) {
                widths[i] = Math.max(widths[i], row[i].toString().length());
            }
        }

        printBorder(widths);
        System.out.print("|");
        for (int i = 0; i < headers.length; i++) {
            System.out.printf(" %" + widths[i] + "s |", headers[i]);
        }
        System.out.println();
        printBorder(widths);

        for (Object[] row : records) {
            System.out.print("|");
            for (int i = 0; i < row.length; i++) {
                System.out.printf(" %" + widths[i] + "s |", row[i]);
            }
            System.out.println();
        }
        printBorder(widths);
        System.out.println("Total rows: " + records.size());
    }

    private void printBorder(int[] widths) {
        System.out.print("+");
        for (int width : widths) {
            for (int i = 0; i < width + 2; i++) System.out.print("-");
            System.out.print("+");
        }
        System.out.println();
    }
}
