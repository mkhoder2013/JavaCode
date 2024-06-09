/* Created by Mazen Khoder 
 * Used for Pipeline demo
 * 2024-JUN-01
 */

package tendered;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

public class IOT_Ingestor {
	// Queue of received statements 
    private static BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

    public static void ingestData() {
    	// Thread for getting the statements and put them in the Queue
        Thread serverThread = new Thread(IOT_Ingestor::startServer);
        serverThread.start();

        while (true) {
            try (Connection connection = IOT_Toolkit.getPostgresConnection()) {
                while (true) {
                	String dataJSON = messageQueue.take();
                	JSONObject json = new JSONObject(dataJSON);
         	
                    String SQL = json.getString("SQL");
                    String ingestCloud = json.getString("ingestCloud");
                    String tableName = json.getString("Tablename");
                    try (Statement stmt = connection.createStatement()) {
                        stmt.executeUpdate(SQL);
                    } catch (SQLException e) {
                        //e.printStackTrace();
                    	System.out.println("Faild statmenet: "+SQL);
                    }
                    if ( ingestCloud.compareTo("YES")==0) {
                    	System.out.println("----------------------------------------");
                    	JSONObject jsonToIngest = new JSONObject();
                        jsonToIngest.put("serial_nb", json.get("serial_nb"));
                        jsonToIngest.put("Temperature", json.get("temp"));
                        jsonToIngest.put("Room Id", json.get("room_id_id"));
                        jsonToIngest.put("In/Out", json.get("out_in"));
                        jsonToIngest.put("Id",json.get("id"));
                        jsonToIngest.put("Date Time",json.get("noted_date"));
                    	IOT_MoveDataToInfluxDB.insertOneRecordInInfluxDB(jsonToIngest);
                        try (Statement stmt = connection.createStatement()) {
                            stmt.executeUpdate(" update "+tableName+ " set uploded_to_cloud='YES' where serial_nb="+json.get("serial_nb")+" ");
                        } catch (SQLException e) {
                            //e.printStackTrace();
                        	System.out.println("Faild to update 'uploded_to_cloud' postgres table: ");
                        }

                    };
                    System.out.println("Queue Size: " + messageQueue.size());

         //  un comment this line for making a delay
         //           TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Retry logic or alert mechanism can be added here
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt(); // Restore interrupt status
                break;
            }
        }
    }

    private static void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(IOT_Toolkit.INGESTOR_PORT)) {
            System.out.println("Ingestor Application is Listening on Port " + IOT_Toolkit.INGESTOR_PORT);

            while (true) {
                try (Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    String data;
                    while ((data = in.readLine()) != null) {
                        messageQueue.put(data);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    // Log error message here
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            // Log error message here
        }
    }

}
