package tendered;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.json.JSONObject;

public class IOT_RecordMeta {
	private ArrayList<IOT_ColumnMeta> listColumns;
	private String regex;

    /**
     * Method to extract values from input string using column regex patterns
     * @param serial_nb 
     * @param inputString The input string to extract values from
     * @return A list of extracted values
     */

    public String generateInsertSQL(String tableName, List<Object> values, long serial_nb) {
        if (values.size() != listColumns.size()) {
            throw new IllegalArgumentException("Values count does not match columns count");
        }

        StringBuilder insertSQL = new StringBuilder("INSERT INTO ");
        insertSQL.append(tableName).append(" (serial_nb,");

        for (int i = 0; i < listColumns.size(); i++) {
            IOT_ColumnMeta column = listColumns.get(i);
            insertSQL.append(column.getName());
            if (i < listColumns.size() - 1) {
                insertSQL.append(", ");
            }
        }

        insertSQL.append(") VALUES ("+serial_nb+",");

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            insertSQL.append(formatValue(value, listColumns.get(i).getDataType()));
            if (i < values.size() - 1) {
                insertSQL.append(", ");
            }
        }

        insertSQL.append(");");
        return insertSQL.toString();
    }
    
    /**
     * Helper method to format values according to their data types
     * @param value The value to format
     * @param dataType The data type of the value
     * @return The formatted value as a string
     */
    private String formatValue(Object value, String dataType) {
        switch (dataType.toLowerCase()) {
            case "text":
            case "timestamp":
            case "timestamptz":
            case "date":
            	String pattern = "(\\d{1,2}-\\d{1,2}-\\d{4})(\\d{1,2}:\\d{1,2})";
                String replacement = "$1 $2";
            	value = ((String) value).replaceAll(pattern, replacement);
                return "'" + value.toString() + "'";
            case "integer":
            case "float":
                return value.toString();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
    /**
     * Method to generate SQL CREATE TABLE statement
     * @param tableName Name of the table to be created
     * @return SQL CREATE TABLE statement as a string
     * @throws SQLException 
     */
    public String generateCreateTableSQL(String tableName) throws SQLException {
    	
		String sql= "DROP TABLE IF EXISTS "+tableName +"  CASCADE ;\n";
	    sql+="CREATE TABLE IF NOT EXISTS "+tableName +"(\nserial_nb SERIAL PRIMARY KEY,\n" ;
        StringBuilder createTableSQL = new StringBuilder(sql);
        for (int i = 0; i < listColumns.size(); i++) {
            IOT_ColumnMeta column = listColumns.get(i);
            createTableSQL.append(column.getName())
                    .append(" ")
                    .append(convertDataTypeToSQL(column.getDataType()));
            if (i < listColumns.size() - 1) {
                createTableSQL.append(",\n");
            }
        }

        createTableSQL.append(", isValid text default 'Valid' ,comments text default '', originalRecord text default '',Uploded_to_cloud text default 'NO'\n);");
	    Statement stmt = null;
	    stmt = IOT_Toolkit.getPostgresConnection().createStatement() ;
	    
		stmt.executeUpdate(createTableSQL.toString());
		stmt = null;
		
 //       System.out.println(createTableSQL.toString());
        return createTableSQL.toString();
    }

    /**
     * Helper method to convert custom data type to SQL data type
     * @param dataType Custom data type
     * @return SQL data type as a string
     */
    private String convertDataTypeToSQL(String dataType) {
        switch (dataType.toLowerCase()) {
            case "text":
                return "TEXT";
            case "integer":
                return "INTEGER";
            case "timestamp":
                return "TIMESTAMP";
            case "float":
                return "REAL";  // Use REAL for floating point numbers in SQL
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
	public IOT_RecordMeta(String metaDataFile ) {
		ObjectMapper objectMapper = new ObjectMapper();
        try {
            // Read the JSON file into a list of IOT_Column objects
            List<IOT_ColumnMeta> columns = objectMapper.readValue(new File(metaDataFile), new TypeReference<List<IOT_ColumnMeta>>(){});

            // Create an IOT_Record object and set the list of columns
            this.setListColumns(columns);

            // Print the IOT_Record object to verify
           
        } catch (IOException e) {
            e.printStackTrace();
        }	
    }

	public String getConcatenatedRegex() {
        StringBuilder concatenatedRegex = new StringBuilder();
        for (IOT_ColumnMeta column : listColumns) {
            if (concatenatedRegex.length() > 0) {
                concatenatedRegex.append(",");
            }
            concatenatedRegex.append(column.getRegex());
        }
        return concatenatedRegex.toString();
    }
	
	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;
	}

	public void setListColumns(ArrayList<IOT_ColumnMeta> listColumns) {
		this.listColumns = listColumns;
	}

	public ArrayList<IOT_ColumnMeta> getListColumns() {
        return listColumns;
    }

    public void setListColumns(List<IOT_ColumnMeta> columns) {
        this.listColumns = (ArrayList<IOT_ColumnMeta>) columns;
        this.regex=getConcatenatedRegex();
    }

    @Override
    public String toString() {
        return "IOT_Record {\n" +
                "listColumns=" + listColumns +
                "\n Regex="+getRegex()+"\n}";
    }

	public void createtableInDB() {
		// TODO Auto-generated method stub
		
	}

	public JSONObject getJSONObject(String line) {
		JSONObject json = new JSONObject();
		
		String[] values = line.split(",", -1);

        if (values.length != listColumns.size()) {
            throw new IllegalArgumentException("Input string does not match the expected format");
        }
        for (int i = 0; i < listColumns.size(); i++) {
            IOT_ColumnMeta column = listColumns.get(i);
            Pattern pattern = Pattern.compile(column.getRegex());
            Matcher matcher = pattern.matcher(values[i]);
            if (matcher.matches()) {
            	values[i]=values[i].replaceAll(column.getRemovetext(), "");
            	json.put(listColumns.get(i).getName(), values[i]);
              
            } else {
                throw new IllegalArgumentException("Value does not match the regex for column: " + column.getName());
            }
        }
        
         return json;
	}

	public List<Object> getListOfValues(String line) {
        List<Object> extractedValues = new ArrayList<>();
        String[] values = line.split(",", -1);

        if (values.length != listColumns.size()) {
            throw new IllegalArgumentException("Input string does not match the expected format");
        }

        for (int i = 0; i < listColumns.size(); i++) {
            IOT_ColumnMeta column = listColumns.get(i);
            Pattern pattern = Pattern.compile(column.getRegex());
            Matcher matcher = pattern.matcher(values[i]);
            if (matcher.matches()) {
            	values[i]=values[i].replaceAll(column.getRemovetext(), "");
                extractedValues.add(values[i]);
            } else {
                throw new IllegalArgumentException("Value does not match the regex for column: " + column.getName());
            }
        }
        return extractedValues;		
	}
	public JSONObject tryCorrectData(String tablename, String data, int serial_nb) {

		  String[] parts = data.split(",");
          JSONObject json = new JSONObject();
	        // Print each part of the split string
		  List<String> values=new ArrayList<String>();
//		  System.out.println("-----------------------VALUES-----------------------");
	      for (String part : parts) {
	    	    part=part.replaceAll("[\\s\\n\\r]", "");
	            values.add(part);
	      }
	        
	      List<IOT_ColumnMeta> regexesColumns=new ArrayList<IOT_ColumnMeta>();
	      
//		  System.out.println("-----------------------REGEXES-----------------------");
	      for (IOT_ColumnMeta c: listColumns) {
	    	  regexesColumns.add(c);
//	    	  System.out.println(c.getRegexExtended());
	      }
	      
	      
	      double[][] matrix = buildMatchingMatrix( values,regexesColumns);
	      
	      
	      String htmlTable=DisplayMatrix(values,matrix);
	      
	      String resultSQL="";
	      String comments="Not valid Columns: ";
	      for (int i=0;i< listColumns.size();i++) {
	    	  boolean found=false;
		      for (int j=0;j<values.size();j++) {
		    	  	if (matrix[j][i]>0.8) {
		    	  		String v=values.get(j);
		    	  		if (listColumns.get(i).getName().compareTo("room_id_id")==0) 
		    	  			v="Room Admin";
		    	  		else
		    	  			if (listColumns.get(i).getName().compareTo("out_in")==0) {
		    	  				if (values.get(j).toLowerCase().contains("n"))
		    	  					v="In";
		    	  				else
		    	  					v="Out";
		    	  			}
		    	  			else
		    	  				if (listColumns.get(i).getName().compareTo("id")==0)
		    	  					v=v.replaceAll(listColumns.get(i).getRemovetext(), "");
		    	  		resultSQL+=listColumns.get(i).getName() +"="+ formatValue(v, listColumns.get(i).getDataType());
		    	  		found=true;
		     	  		if (i < listColumns.size() - 1) {
		    	  			resultSQL+=", ";
		                }
		    	  		break;
		    	  	}
		    	  	
		        }
		      if (!found) comments+=listColumns.get(i).getName()+", ";
 
	      }
	      
//	      System.out.println("=========================="+comments);	 
	      if (resultSQL!="") {
	    	  if (resultSQL.trim().endsWith(",")) {
		    	  resultSQL = resultSQL.substring(0, resultSQL.length() - 2);
		      }
	    	  resultSQL=" update "+tablename+" set \"comments\"='"+comments+"',"+resultSQL +" where serial_nb="+serial_nb+"";
	      }
	      else
	    	  resultSQL=" update "+tablename+" set \"comments\"='"+comments+"'  where serial_nb="+serial_nb+"";
	    	
//	      System.out.println("+++++++++++++++++++"+resultSQL);	
	      json.put("htmlTable", htmlTable);
	      json.put("SQL", resultSQL);
	      return json; 
	}
	
	 private String DisplayMatrix(List<String> values, double[][] matrix) {
		  String htmlTable="<Table border='1'>";
	  	  htmlTable +="<tr>";
	  	  htmlTable +="<td>";
	  	  htmlTable +="";
	  	  htmlTable +="</td>";
	  	  for (IOT_ColumnMeta c: listColumns) {
		  	  htmlTable +="<td>";
		  	  htmlTable +=c.getName();
		  	  htmlTable +="</td>";
	      }
	  	  htmlTable +="</tr>";
	  	  htmlTable +="<tr>";
	  	  htmlTable +="<td>";
	  	  htmlTable +="";
	  	  htmlTable +="</td>";
	  	  for (IOT_ColumnMeta c: listColumns) {
		  	  htmlTable +="<td>";
		  	  htmlTable +=c.getRegexExtended();
		  	  htmlTable +="</td>";
	      }
	  	  htmlTable +="</tr>";
	  	  
	      int i=0;
	      for (double[] row : matrix) {
	    	  	htmlTable +="<tr>";
	    	  	htmlTable +="<td>";
	    	  	htmlTable +=values.get(i);
	    	  	htmlTable +="</td>";
	    	    
	    	  //	System.out.print(values.get(i)+"\t\t\t\t |") ;
               i++;
	            for (double score : row) {
	            	String style=(score==1)?"style='background-color:green'":"" ;
		    	  	htmlTable +="<td "+ style +" >";
		    	  	htmlTable +=String.format("%.4f", score);;
		    	  	htmlTable +="</td>";	            	
	          //      System.out.printf( "%.2f \t ", score);

	            }
//	            System.out.println();
	            htmlTable +="<tr>";

	        }
	       htmlTable+="</Table>";
	      	// TODO Auto-generated method stub
		return htmlTable;
	}

	public static double[][] buildMatchingMatrix( List<String> strings,List<IOT_ColumnMeta> column) {
	        int regexCount = column.size();
	        int stringCount = strings.size();
	        double[][] matrix = new double[stringCount][regexCount];

	        for (int i = 0; i < stringCount; i++) {
	        	String str = strings.get(i);
	            for (int j = 0; j <regexCount ; j++) {
	            	Pattern pattern = Pattern.compile(column.get(j).getRegexExtended());
	               
	                matrix[i][j] = calculateSimilarityScore(pattern, str);
	            }
	        }

	        return matrix;
	    }
	    public static double calculateSimilarityScore(Pattern pattern, String str) {
	        Matcher matcher = pattern.matcher(str);

	        // Check for full match
	        if (matcher.matches()) {
	            return 1.0; // Full match
	        }

	        // Calculate partial match score
	        double partialScore = 0;
	        while (matcher.find()) {
	            String match = matcher.group();
	            double matchScore = (double) match.length() / str.length();
	            partialScore = Math.max(partialScore, matchScore);
	        }

	        // Calculate Levenshtein distance score
	        LevenshteinDistance levenshtein = new LevenshteinDistance();
	        String patternStr = pattern.pattern();
	        int distance = levenshtein.apply(patternStr, str);
	        double normalizedDistance = 1.0 - ((double) distance / Math.max(patternStr.length(), str.length()));

	        // Combine partial match score and edit distance score
	        return (partialScore + normalizedDistance) / 2;
	    }
		public String generateInsertSQLNullvalus(String tbleName,String originalRecord, long serial_nb) {
	        StringBuilder insertSQL = new StringBuilder("INSERT INTO ");
	        insertSQL.append(tbleName).append(" (serial_nb,isvalid,originalrecord,");

	        for (int i = 0; i < listColumns.size(); i++) {
	            IOT_ColumnMeta column = listColumns.get(i);
	            insertSQL.append(column.getName());
	            if (i < listColumns.size() - 1) {
	                insertSQL.append(", ");
	            }
	        }

	        insertSQL.append(") VALUES ("+serial_nb+",'InValid','"+originalRecord+"',");

	        for (int i = 0; i < listColumns.size(); i++) {
	            insertSQL.append("null");
	            if (i < listColumns.size() - 1) {
	                insertSQL.append(", ");
	            }
	        }

	        insertSQL.append(");");
	        return insertSQL.toString();
		}
}
