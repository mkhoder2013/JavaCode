/*
 * a class for making some operations on influx DB
 * ** Insert batch of data
 * ** Insert one record
 * ** Delete data
 * ** Delete measurement 
 */
package tendered;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.json.JSONObject;

import com.influxdb.client.DeleteApi;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

public class IOT_MoveDataToInfluxDB {
	 public static void insertBatchInInfluxDB() {
	        // Create InfluxDB client
	        InfluxDBClient client = 
	            InfluxDBClientFactory.create(IOT_Toolkit.influx_url, 
	                                         IOT_Toolkit.influx_token.toCharArray());

	        // Configure write options for batching
	        WriteOptions writeOptions = WriteOptions.builder()
	                .batchSize(500)         // Number of points per batch
	                .flushInterval(1000)    // Time to flush batch (in milliseconds)
	                .bufferLimit(10000)     // Buffer limit for points
	                .build();

	        // Example record data
	        double temperature;
	        double pressure;
	        double humidity;
	        String location = "Stairs";    
	        String user = "Mazen";    
	        Random random = new Random();

	        // Create WriteApi with batching options
	        try (WriteApi writeApi = client.makeWriteApi(writeOptions)) {
	            for (int i = 0; i < 1000; i++) {
	                temperature = -10 + (50 * random.nextDouble());
	                pressure = 950 + (100 * random.nextDouble());
	                humidity = 100 * random.nextDouble();
	                System.out.println("Generating point " + i);
	                Point point = Point
	                    .measurement("Environmental_Data")
	                    .addTag("location", location)
	                    .addField("temperature", temperature)
	                    .addField("pressure", pressure)
	                    .addField("humidity", humidity)
	                    .addField("user", user)
	                    .time(Instant.now(), WritePrecision.NS);

	                writeApi.writePoint(IOT_Toolkit.influx_bucket, IOT_Toolkit.influx_org, point);
	                System.out.println("Written point " + i + ": " + point.toLineProtocol());
	            }

	            // Ensure all data is flushed before closing the client
	            writeApi.flush();
	            System.out.println("Flushed data to InfluxDB Cloud");
	        } catch (Exception e) {
	            e.printStackTrace();
	        } finally {
	            client.close();
	            System.out.println("Data written to InfluxDB Cloud");
	        }
	    }


	public static void insertOneRecordInInfluxDB(JSONObject jsonToIngest) {
	 
	        InfluxDBClient client = 
	        		InfluxDBClientFactory.create(IOT_Toolkit.influx_url, 
	        				                     IOT_Toolkit.influx_token.toCharArray(), 
	        				                     IOT_Toolkit.influx_org, 
	        				                     IOT_Toolkit.influx_bucket);


            
	        double temperature = jsonToIngest.getDouble("Temperature");
	        String location = jsonToIngest.getString("In/Out");
	        String id = jsonToIngest.getString("Id");
	        String roomId = jsonToIngest.getString("Room Id");
	        long serial_nb = jsonToIngest.getInt("serial_nb");
	        String noted_date = jsonToIngest.getString("Date Time");
	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm");
	        LocalDateTime localDateTime = LocalDateTime.parse(noted_date, formatter);
	        ZoneOffset offset = ZoneOffset.UTC;
	        OffsetDateTime offsetDateTime = localDateTime.atOffset(offset);
	        Instant instant = offsetDateTime.toInstant();
	        

		        try (WriteApi writeApi = client.getWriteApi()) {
		            Point point = Point
		            		.measurement("Environmental_Data")
		                    .addTag("Location", location)
		                    .addField("Temperature", temperature)
		                    .addField("Id", id)
		                    .addField("Room", roomId)
		                    .addField("serial_nb", serial_nb)
		                    .time(Instant.now(), WritePrecision.NS);
//                    		.time(instant, WritePrecision.NS);
		 
		            writeApi.writePoint(point);
		 
		            System.out.println("Data written to InfluxDB Cloud");
		      
	        }
	 
	        client.close();
	    }	
	
	public static void deleteDataFromInfluxDB() {
        // Create InfluxDB client
        InfluxDBClient client = 
            InfluxDBClientFactory.create(IOT_Toolkit.influx_url, 
                                         IOT_Toolkit.influx_token.toCharArray());

        // Define the time range for deletion
        OffsetDateTime start = OffsetDateTime.parse("2023-01-01T00:00:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        OffsetDateTime stop = OffsetDateTime.parse("2024-12-01T00:00:00Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        // Create DeleteApi instance
        DeleteApi deleteApi = client.getDeleteApi();

        try {
            // Delete data within the specified time range
            deleteApi.delete(start, stop, "", IOT_Toolkit.influx_bucket, IOT_Toolkit.influx_org);
            System.out.println("Data deleted from InfluxDB bucket.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the client
            client.close();
        }
    }
	
	public static void deleteMeasurementFromInfluxDB() {
        // Create InfluxDB client
        InfluxDBClient client = 
            InfluxDBClientFactory.create(IOT_Toolkit.influx_url, 
                                         IOT_Toolkit.influx_token.toCharArray());

        String measurement = "Environmental_Data";

        // Create the flux query to drop the measurement
        String flux = "from(bucket: \"" + IOT_Toolkit.influx_bucket + "\") |> range(start: 0) |> dropMeasurement(name: \"" + measurement + "\")";

        try {
            // Execute the flux query
            client.getQueryApi().query(flux, IOT_Toolkit.influx_org);
            System.out.println("Measurement dropped from InfluxDB bucket.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the client
            client.close();
        }
    }

}
