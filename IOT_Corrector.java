package tendered;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
/*
 *  Node 2: correct data and send to Node 3 (ingestor)
 */
public class IOT_Corrector {
   // Queue of data
    private static BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

    public static void correctData(String metaDataFile) throws Exception {
    	
    	Connection c=(Connection) IOT_Toolkit.getPostgresConnection();
    	
    	// fire a thread which receive data and put it in the queue
        Thread serverThread = new Thread(() -> startServer());
        serverThread.start();
        // A delay of 2 seconds to garantee that pipe line started 
		IOT_RecordMeta rcordMetadata=new IOT_RecordMeta(metaDataFile);
		try {
			TimeUnit.MILLISECONDS.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
		// Initialize HTML content

        while (true) {
            try {
                String dataJSON = messageQueue.take();
                System.out.println("Received: " + dataJSON);
                System.out.println("Queue Size: " + messageQueue.size());
                
                JSONObject json = new JSONObject(dataJSON);

                // Extract values
                String tablename = json.getString("Tablename");
                String data = json.getString("Data");
                String statmentType= json.getString("StatmentType");
                String SQL="";
                int serial_nb = json.getInt("serial_nb");
                // the function tryCorrectData will return a data after correction if exist
                JSONObject correctionResul=rcordMetadata.tryCorrectData(tablename,json.getString("Data"),serial_nb);
                
                json.put("SQL", correctionResul.get("SQL"));
                json.put("ingestCloud","NO");
                json.put("Tablename",tablename);
                
                sendDataToIngestor(json.toString());

                System.out.println(correctionResul.get("htmlTable"));
                
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
    }
    private static void sendDataToIngestor(String data) throws Exception {
        try (Socket socket = new Socket(IOT_Toolkit.INGESTOR_HOST, IOT_Toolkit.INGESTOR_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(data);
        }
    }
    private static void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(IOT_Toolkit.CORRECTOR_PORT)) {
            System.out.println("Corrector Application is Listening on Port " + IOT_Toolkit.CORRECTOR_PORT);

            while (true) {
                try (Socket socket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    String data;
                    while ((data = in.readLine()) != null) {
                        messageQueue.put(data);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
}
