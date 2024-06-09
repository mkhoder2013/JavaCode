package tendered;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

public class IOT_ReaderApp {


    public static void realTimeSimulate(String metaDataFile,String tbleName,String FILE_PATH) throws SQLException {
 
        String regex="" ;
        // reaad the metadata file
		IOT_RecordMeta rcordMetadata=new IOT_RecordMeta(metaDataFile);
		
        try {
			TimeUnit.MILLISECONDS.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		rcordMetadata.generateCreateTableSQL(tbleName);
		// generate the complete regex of a line
		regex=rcordMetadata.getConcatenatedRegex();
        Pattern pattern = Pattern.compile(regex);

        while (true) {
            try {
                try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
                    String line;
                    line = br.readLine(); // Ignore the header line
                    long serial_nb=1;
                    while ((line = br.readLine()) != null) {
                    	Matcher matcher = pattern.matcher(line);
                    	// The line is valid
                        if (matcher.matches()) {
                            List<Object> values=rcordMetadata.getListOfValues(line);
                            String SQL= rcordMetadata.generateInsertSQL(tbleName,values,serial_nb);
                            
                            JSONObject jsonToIngest = rcordMetadata.getJSONObject(line);
                            jsonToIngest.put("SQL", SQL);
                            jsonToIngest.put("serial_nb", serial_nb);
                            jsonToIngest.put("StatmentType", "INSERT");
                            jsonToIngest.put("ingestCloud","YES");
                            jsonToIngest.put("Tablename",tbleName);
                            System.out.println(jsonToIngest);
                            // send statement to Ingestor Node 3 with data
                            sendDataToIngestor(jsonToIngest.toString());
 
                        } // The line is invalid
                        else {
                            String SQL= rcordMetadata.generateInsertSQLNullvalus(tbleName,line,serial_nb);
                            JSONObject jsonToIngest = new JSONObject();
                            jsonToIngest.put("SQL", SQL);
                            jsonToIngest.put("StatmentType", "INSERT");
                            jsonToIngest.put("ingestCloud","NO");
                            jsonToIngest.put("Tablename",tbleName);
                            System.out.println(jsonToIngest);
                         // send statement to Ingestor Node 3 but with null values
                            sendDataToIngestor(jsonToIngest.toString());
                            JSONObject jsonToCorrector = new JSONObject();
                            jsonToCorrector.put("Tablename", tbleName);
                            jsonToCorrector.put("Data", line);
                            jsonToCorrector.put("serial_nb", serial_nb);
                            jsonToCorrector.put("StatmentType", "UPDATE");
                            sendDataToCorrector(jsonToCorrector.toString());
                        }                    	
                        serial_nb++;
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                    break;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendDataToCorrector(String data) throws Exception {
        try (Socket socket = new Socket(IOT_Toolkit.CORRECTOR_HOST, IOT_Toolkit.CORRECTOR_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(data);
        }
    }
    private static void sendDataToIngestor(String data) throws Exception {
        try (Socket socket = new Socket(IOT_Toolkit.INGESTOR_HOST, IOT_Toolkit.INGESTOR_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(data);
        }
    }
}
