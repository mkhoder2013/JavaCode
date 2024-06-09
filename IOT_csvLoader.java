package tendered;
// This class not used 
// It is for uploading csv file into local postgres
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import ch.hearc.te.data.MyConnection;

public class IOT_csvLoader {
	private String fullPath;
	private String fullTblName;
	private int 	batchSize;
	public  ArrayList<String> columnsNamesList;
	private Connection dbConnection;

	
	/**
	 * @param fullPath
	 * @param fullTblName
	 * @param batchSize
	 * @throws SQLException 
	 */
	
	public void load() throws SQLException {
		System.out.println("------ Load file: "+getFullPath());
		createObjectInSchema_forCSV(getFullTblName());
		QuickFileRead();
	
	}
	public ArrayList<String> getColumnsNamesList() {
		return columnsNamesList;
	}
	public void setColumnsNamesList(ArrayList<String> columnsNamesList) {
		this.columnsNamesList = columnsNamesList;
	}
	public Connection getDbConnection() {
		return dbConnection;
	}
	public void setDbConnection(Connection dbConnection) {
		this.dbConnection = dbConnection;
	}
	public  void QuickFileRead() throws SQLException {
		
		java.sql.Statement stmt = null;
		String sql="";
		//----------  Add columns
		java.sql.Statement addColumnsstmt = null;
		java.sql.Statement updateColumnsstmt = null;
		addColumnsstmt = getDbConnection().createStatement();

		stmt=null;
        try {
            List<String> lines = Files.readAllLines(Paths.get(fullPath));
            int i=0;
            
            String insertSQL = "INSERT INTO \"Tendered\".test (r) VALUES (?);";
           
            int count = 0;
            PreparedStatement pstmt = getDbConnection().prepareStatement(insertSQL);

            for (String line : lines) {
            	
            	
            	if (i>0) {  // Pass over the first line
            		pstmt.setString(1, line);
                    pstmt.addBatch();
            	}
                i++;
                count++;
                if (count % batchSize == 0 ) {
             //       pstmt.executeBatch();
                    count=0;
                }
                
            	
                System.out.println((i)+"  "+line);
            }
            
            if (count>0) {
        //    	pstmt.executeBatch();
                System.out.println("Executed remaining batch of size: " + count);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    
}
	public  void createObjectInSchema_forCSV(String tblName ) throws SQLException  {
		String line = "";  
		String splitBy = ","; 
	    Statement stmt = null;
	    stmt = getDbConnection().createStatement();
	    System.out.println(tblName);
		try   
		{  
		    //parsing a CSV file into BufferedReader class constructor  
			BufferedReader br = new BufferedReader(new FileReader(getFullPath()));  
			
			line =br.readLine();
			String[] firstrow = line.split(splitBy);
			for (int i=0;i<firstrow.length;i++)
				if ( columnsNamesList.contains(firstrow[i].trim().toUpperCase()) ) {
	    			columnsNamesList.add((firstrow[i].trim()+i).toUpperCase());
	    			System.out.println("Found column:"+(firstrow[i].trim()+i).toUpperCase());
				}
	    		else {
	                columnsNamesList.add(firstrow[i].trim().toUpperCase());
	                System.out.println("Not Found column:"+firstrow[i].trim().toUpperCase());
	    		}
			br=null;
		}
		catch (IOException e)   
		{  
			e.printStackTrace();  
		}  	    

		String sql= " DROP TABLE IF EXISTS "+tblName +"  CASCADE ; ";
		       sql+=" CREATE TABLE IF NOT EXISTS "+tblName +" "+ getCreateTableSQL();

		System.out.println(sql);
		stmt.executeUpdate(sql);

		stmt = null;
	}
	public String getCreateTableSQL() {
		String sql="( ";
		String comma;
		for (int i=0;i<columnsNamesList.size();i++ ) {
			comma= i==columnsNamesList.size()-1? "  ":" , ";
			sql+=columnsNamesList.get(i).replaceAll("[^a-zA-Z0-9]", "_").toUpperCase() + " text "+comma;
		}
		sql+=",Serial_NB text,file_name text )".toUpperCase();
		return sql;
	}
	public IOT_csvLoader(String fullPath, String fullTblName, int batchSize) {
		super();
		this.fullPath = fullPath;
		this.fullTblName = fullTblName;
		this.batchSize = batchSize;
		dbConnection=new MyConnection().getConnection();
		columnsNamesList= new ArrayList<String>();
	}
	public String getFullPath() {
		return fullPath;
	}
	public void setFullPath(String fullPath) {
		this.fullPath = fullPath;
	}
	public String getFullTblName() {
		return fullTblName;
	}
	public void setFullTblName(String fullTblName) {
		this.fullTblName = fullTblName;
	}
	public int getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	

}
