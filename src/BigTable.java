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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/*
 * Use Google Bigtable to store and analyze sensor data.
 */
public class BigTable {
    // TODO: Fill in information for your database
    public final String projectId = "bigtabledemo-463315";
    public final String instanceId = "assignment4";
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather"; // TODO: Must change table name if sharing my database

    public BigtableDataClient dataClient;
    public BigtableTableAdminClient adminClient;

    public static void main(String[] args) throws Exception {
        BigTable testBigTable = new BigTable();
        testBigTable.run();
    }

    public void connect() throws IOException {
        // TODO: Write code to create a data client and admin client to connect to Google Bigtable
        // Create settings for the data client
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();

        // Create settings for the admin client
        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();

        // Create the clients
        dataClient = BigtableDataClient.create(dataSettings);
        adminClient = BigtableTableAdminClient.create(adminSettings);

        System.out.println("Connected to Bigtable instance: " + instanceId);
    }

    public void run() throws Exception {
        connect();

        // TODO: Comment or uncomment these as you proceed. Once load data, comment them out.
        deleteTable();
        createTable();
        loadData();

        int temp = query1();
        System.out.println("Temperature: " + temp);

        int windSpeed = query2();
        System.out.println("WindSpeed: " + windSpeed);

        ArrayList<Object[]> data = query3();
        System.out.println("\n=== Query 3 Results: All readings for SeaTac on October 2, 2022 ===");
        printTableFormat(data);

        temp = query4();
        System.out.println("Temperature: " + temp);

        query5();

        close();
    }

    /**
     * Close data and admin clients
     */
    public void close() {
        dataClient.close();
        adminClient.close();
    }

    public void createTable() {
        // TODO: Create a table to store sensor data.
        try {
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId)
                    .addFamily(COLUMN_FAMILY);

            adminClient.createTable(createTableRequest);
            System.out.println("Table " + tableId + " created successfully");
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
        }
    }

    /**
     * Loads data into database.
     * Data is in CSV files. Note that must convert to hourly data.
     * Take the first reading in a hour and ignore any others.
     */
    public void loadData() throws Exception {
        String path = "src/bin/data/";

        // TODO: Load data from CSV files into sensor table
        try {
            // SeaTac station id is SEA
            System.out.println("Load data for SeaTac");
            loadStationData(path + "seatac.csv", "SEA");

            // Vancouver station id is YVR
            System.out.println("Loading data for Vancouver");
            loadStationData(path + "vancouver.csv", "YVR");

            // Portland station id is PDX
            System.out.println("Loading data for Portland");
            loadStationData(path + "portland.csv", "PDX");

        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    private void loadStationData(String filename, String stationId) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        boolean isHeader = true;
        Map<String, Boolean> hourlyDataLoaded = new HashMap<>();
        BulkMutation bulkMutation = BulkMutation.create(TableId.of(tableId));

        while ((line = reader.readLine()) != null) {
            // Skip header rows
            if (isHeader) {
                if (line.contains("Date,Time")) {
                    isHeader = false;
                }
                continue;
            }

            String[] parts = line.split(",");
            if (parts.length < 9) continue;

            String date = parts[1].trim();
            String time = parts[2].trim();

            // Extract hour from time (HH:MM format)
            int hourInt = Integer.parseInt(time.split(":")[0]);
            String hour = String.format("%02d", hourInt);  // "00", "01", "02", etc.
            String hourKey = date + "-" + hour;

            // Skip if we already have data for this hour
            if (hourlyDataLoaded.containsKey(hourKey)) {
                continue;
            }
            hourlyDataLoaded.put(hourKey, true);

            // Create row key: stationId#date#hour
            String rowKey = stationId + "#" + date + "#" + hour;

            // Parse data values
            String temperature = parts[3].trim();
            String dewPoint = parts[4].trim();
            String humidity = parts[5].trim();
            String windSpeed = parts[6].trim();
            String gust = parts[7].trim();
            String pressure = parts[8].trim();

            // Create mutations for this row
            Mutation mutation = Mutation.create()
                    .setCell(COLUMN_FAMILY, "temperature", temperature)
                    .setCell(COLUMN_FAMILY, "dewPoint", dewPoint)
                    .setCell(COLUMN_FAMILY, "humidity", humidity)
                    .setCell(COLUMN_FAMILY, "windSpeed", windSpeed)
                    .setCell(COLUMN_FAMILY, "gust", gust)
                    .setCell(COLUMN_FAMILY, "pressure", pressure)
                    .setCell(COLUMN_FAMILY, "time", time);

            bulkMutation.add(rowKey, mutation);
        }

        reader.close();

        // Execute bulk mutation
        dataClient.bulkMutateRows(bulkMutation);
        System.out.println("Loaded data for station: " + stationId);
    }

    /**
     * Query returns the temperature at Vancouver on 2022-10-01 at 10 a.m.
     */
    public int query1() {
        System.out.println("Executing query #1.");

        // Row key: YVR#2022-10-01#10
        String rowKey = "YVR#2022-10-01#10";

        Row row = dataClient.readRow(TableId.of(tableId), rowKey);
        if (row != null) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                String tempStr = cell.getValue().toStringUtf8();
                return Integer.parseInt(tempStr);
            }
        }

        return 0;
    }

    /**
     * Query returns the highest wind speed in the month of September 2022 in Portland.
     */
    public int query2() {
        System.out.println("Executing query #2.");
        int maxWindSpeed = 0;

        // Create query for Portland in September 2022
        Query query = Query.create(TableId.of(tableId))
                .range(ByteStringRange.create("PDX#2022-09-01", "PDX#2022-09-31"));

        ServerStream<Row> rows = dataClient.readRows(query);

        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "windSpeed")) {
                String windSpeedStr = cell.getValue().toStringUtf8();
                if (!windSpeedStr.equals("M")) { // M means missing data
                    try {
                        int windSpeed = Integer.parseInt(windSpeedStr);
                        if (windSpeed > maxWindSpeed) {
                            maxWindSpeed = windSpeed;
                        }
                    } catch (NumberFormatException e) {
                        Logger.getGlobal().warning("Invalid windSpeed value: " + windSpeedStr);
                    }
                }
            }
        }

        return maxWindSpeed;
    }

    /**
     * Query returns all the readings for SeaTac for October 2, 2022.
     */
    public ArrayList<Object[]> query3() {
        System.out.println("Executing query #3.");
        ArrayList<Object[]> data = new ArrayList<>();

        // Create query for SeaTac on October 2, 2022
        Query query = Query.create(TableId.of(tableId))
                .range(ByteStringRange.create("SEA#2022-10-02#00", "SEA#2022-10-02#24"));

        ServerStream<Row> rows = dataClient.readRows(query);

        for (Row row : rows) {
            // Extract date and hour from row key
            String rowKey = row.getKey().toStringUtf8();
            String[] keyParts = rowKey.split("#");
            String date = keyParts[1];
            String hour = keyParts[2];

            // Get all sensor values
            String temperature = "";
            String dewpoint = "";
            String humidity = "";
            String windSpeed = "";
            String pressure = "";

            for (RowCell cell : row.getCells()) {
                String qualifier = cell.getQualifier().toStringUtf8();
                String value = cell.getValue().toStringUtf8();

                switch (qualifier) {
                    case "temperature":
                        temperature = value;
                        break;
                    case "dewPoint":
                        dewpoint = value;
                        break;
                    case "humidity":
                        humidity = value;
                        break;
                    case "windSpeed":
                        windSpeed = value;
                        break;
                    case "pressure":
                        pressure = value;
                        break;
                }
            }

            // Create object array with required fields
            Object[] rowData = new Object[]{
                    date,
                    hour,
                    Integer.parseInt(temperature),
                    Integer.parseInt(dewpoint),
                    humidity,
                    windSpeed,
                    pressure
            };

            data.add(rowData);
        }

        return data;
    }

    /**
     * Query returns the highest temperature at any station in the summer months of 2022 (July (7), August (8)).
     */
    public int query4() {
        System.out.println("Executing query #4.");
        int maxTemp = -100;

        // Check all three stations for July and August
        String[] stations = {"PDX", "SEA", "YVR"};

        for (String station : stations) {
            // Query for July
            Query julyQuery = Query.create(TableId.of(tableId))
                    .range(ByteStringRange.create(station + "#2022-07-01", station + "#2022-07-32"));

            maxTemp = getMaxTemp(maxTemp, julyQuery);

            // Query for August
            Query augustQuery = Query.create(TableId.of(tableId))
                    .range(ByteStringRange.create(station + "#2022-08-01", station + "#2022-08-32"));

            maxTemp = getMaxTemp(maxTemp, augustQuery);
        }

        return maxTemp;
    }

    private int getMaxTemp(int maxTemp, Query query) {
        ServerStream<Row> julyRows = dataClient.readRows(query);

        for (Row row : julyRows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                String tempStr = cell.getValue().toStringUtf8();
                try {
                    int temp = Integer.parseInt(tempStr);
                    if (temp > maxTemp) {
                        maxTemp = temp;
                    }
                } catch (NumberFormatException e) {
                    Logger.getGlobal().warning("Invalid temperature value: " + tempStr);
                }
            }
        }
        return maxTemp;
    }

    /**
     * Create your own query and test case demonstrating some different.
     * This query finds the average humidity across all stations for a specific date (2022-09-15)
     */
    public void query5() {
        System.out.println("Executing query #5 - Average humidity across all stations on 2022-09-15");

        String targetDate = "2022-09-15";
        String[] stations = {"PDX", "SEA", "YVR"};
        int totalHumidity = 0;
        int count = 0;

        // Query each station for the specific date
        for (String station : stations) {
            Query query = Query.create(TableId.of(tableId))
                    .range(ByteStringRange.create(
                            station + "#" + targetDate + "#00",
                            station + "#" + targetDate + "#24"
                    ));

            ServerStream<Row> rows = dataClient.readRows(query);

            for (Row row : rows) {
                for (RowCell cell : row.getCells(COLUMN_FAMILY, "humidity")) {
                    String humidityStr = cell.getValue().toStringUtf8();
                    try {
                        double humidity = Double.parseDouble(humidityStr);
                        totalHumidity += (int) humidity;
                        count++;
                    } catch (NumberFormatException e) {
                        Logger.getGlobal().warning("Invalid humidity value: " + humidityStr);
                    }
                }
            }
        }

        // Calculate and return average
        if (count > 0) {
            int avgHumidity = totalHumidity / count;
            System.out.println("Found " + count + " humidity readings across all stations");
            System.out.println("Average humidity on " + targetDate + ": " + avgHumidity + "%");
        } else {
            System.out.println("No humidity data found for " + targetDate);
        }
    }

    /**
     * Delete the table from Bigtable.
     */
    public void deleteTable() {
        System.out.println("\nDeleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.printf("Table %s deleted successfully%n", tableId);
        } catch (NotFoundException e) {
            System.err.println("Failed to delete a non-existent table: " + e.getMessage());
        }
    }

    /*
     Credits: took help from ChatGPT for this function which helps in printing
     the data in a tabular format since there is a lot of data
    */
    private void printTableFormat(ArrayList<Object[]> data) {
        if (data.isEmpty()) {
            System.out.println("No data found.");
            return;
        }

        // Define headers
        String[] headers = {"Date", "Hour", "Temperature", "Dewpoint", "Humidity", "Windspeed", "Pressure"};

        // Calculate column widths
        int[] columnWidths = new int[headers.length];

        // Initialize with header lengths
        for (int i = 0; i < headers.length; i++) {
            columnWidths[i] = headers[i].length();
        }

        // Check data for maximum widths
        for (Object[] row : data) {
            for (int i = 0; i < row.length && i < columnWidths.length; i++) {
                int length = row[i].toString().length();
                if (length > columnWidths[i]) {
                    columnWidths[i] = length;
                }
            }
        }

        // Add padding
        for (int i = 0; i < columnWidths.length; i++) {
            columnWidths[i] += 2;
        }

        // Print table border
        printTableBorder(columnWidths);

        // Print headers
        System.out.print("|");
        for (int i = 0; i < headers.length; i++) {
            System.out.printf(" %-" + (columnWidths[i] - 1) + "s|", headers[i]);
        }
        System.out.println();

        // Print separator
        printTableBorder(columnWidths);

        // Print data rows
        for (Object[] row : data) {
            System.out.print("|");
            for (int i = 0; i < row.length && i < columnWidths.length; i++) {
                System.out.printf(" %-" + (columnWidths[i] - 1) + "s|", row[i].toString());
            }
            System.out.println();
        }

        // Print bottom border
        printTableBorder(columnWidths);

        // Print summary
        System.out.println("Total records: " + data.size());
    }

    // Helper method to print table borders
    private void printTableBorder(int[] columnWidths) {
        System.out.print("+");
        for (int width : columnWidths) {
            for (int i = 0; i < width; i++) {
                System.out.print("-");
            }
            System.out.print("+");
        }
        System.out.println();
    }
}