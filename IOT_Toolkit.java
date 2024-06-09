/*
 * A toolkit class for constants
 */

package tendered;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IOT_Toolkit {
	// Connection to postgres
	public static Connection postgres_connection = null;
	// name of service in docker
	public static final String POSTGRES_SERVER = "postgresServer";
//	public static final String POSTGRES_SERVER = "localhost";

	// ports of application
	public static final String CORRECTOR_HOST = "correctorHost";
//	public static final String CORRECTOR_HOST = "localhost";
	public static final int    CORRECTOR_PORT = 5000;

	public static final String INGESTOR_HOST = "ingestorHost";
//	public static final String INGESTOR_HOST = "localhost";
	public static final int    INGESTOR_PORT = 5001;
	
	// influx DB connection information
	public static final String influx_token = "WxK6_w_SOKhB1BNbwvH8vWCh-jR3pf6YyiY5Lms9Uhye7XNE3RoCF-fi-jBpmkZrC3GGmYMaXEHcQCHF2A87mg=="; 
	// Use the token from your InfluxDB Cloud account
	public static final String influx_bucket = "Weather-backet";
	public static final String influx_org = "Data Team";
	public static final String influx_url = "https://us-east-1-1.aws.cloud2.influxdata.com"; 
	public static final String SCHEMA_NAME = "tendered";

	// A method for creating a connection to postgres DB
	public static Connection getPostgresConnection() {
		  
	    try {
			postgres_connection = DriverManager.getConnection("jdbc:postgresql://"+POSTGRES_SERVER+":5432/postgres","postgres", "Temp@123");
			return postgres_connection;
	    } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	     return null;
	}


	public static void createSchema(String schemaName) throws SQLException {
	    Connection connection = null;
	    Statement statement = null;
	    ResultSet resultSet = null;	
	    
	    connection =IOT_Toolkit.getPostgresConnection();
	    
        try {
            // Step 3: Execute a query to check if the schema exists
            System.out.println("Checking if schema exists...");
            statement = connection.createStatement();
            String checkSchemaSQL = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + SCHEMA_NAME + "'";
            resultSet = statement.executeQuery(checkSchemaSQL);

            if (resultSet.next()) {
                System.out.println("Schema " + SCHEMA_NAME + " already exists.");
            } else {
                // Step 4: Execute a query to create the schema if it doesn't exist
                System.out.println("Creating schema...");
                String createSchemaSQL = "CREATE SCHEMA " + SCHEMA_NAME;
                statement.executeUpdate(createSchemaSQL);
                System.out.println("Schema created successfully...");
            }

        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
          //  e.printStackTrace();
        } finally {
            // Finally block used to close resources
            try {
                if (resultSet != null) resultSet.close();
            } catch (SQLException se2) {
            } // nothing we can do
            try {
                if (statement != null) statement.close();
            } catch (SQLException se2) {
            } // nothing we can do
            try {
                if (connection != null) connection.close();
            } catch (SQLException se) {
                se.printStackTrace();
            } // end finally try
        } // end try
		
	}
}
