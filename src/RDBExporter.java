package org.bigradzebra.experience;
import java.sql.*;
import java.io.*;
import java.util.*;

public class RDBExporter {

	private List<String> RESERVE_WORDS ;
	private static String SEP="\001";
	private Connection CONN ;
        private String PARTITION_VALUE="";
	/**
	 * @param args
	 */
	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		println("[demo] export RDB data to folder: export_folder");
		RDBExporter exporter = new RDBExporter("jdbc:sqlite:itTalent.db","org.sqlite.JDBC");
		exporter.exportToFolder("export_folder");
		println("[demo] general HIVE ddl: hive.ddl");
		exporter.generateHiveDDL("hive.ddl");
		println("[demo] general HIVE import file command to folder: export_folder ");
		exporter.exportImportCliToFolder("export_folder","2012-oo-xx","hive.cli");
	}
	public static void println(Object msg){
		System.out.println(msg);
	}

	public RDBExporter(String dburi,String driver)throws Exception{
		Class.forName(driver);	
		this.CONN =
	    	      DriverManager.getConnection(dburi);
		this.RESERVE_WORDS = new ArrayList<String>();
		this.RESERVE_WORDS.add("desc");
		this.RESERVE_WORDS.add("select");

	}
	/** generage hive DDL */
        public void generateHiveDDL(String outputFileName)throws Exception{
            
        	StringBuffer ddl = new StringBuffer();

		 PrintWriter ddlout =
                             new PrintWriter(
                                new BufferedWriter(
                                        new FileWriter(outputFileName)
                                )
                             );


		List<String> tableNames = this.getTableNames() ;
                Iterator<String> tableNamesIter = tableNames.iterator() ;
                while(tableNamesIter.hasNext() ) {
			String tableName =  tableNamesIter.next() ;
                    	ddl.append("CREATE EXTERNAL TABLE " +tableName +"( ");
			List<String> columns = getColumnNames(tableName) ;
 			Iterator<String> columnsNamesIter = columns.iterator() ;
	 		while(columnsNamesIter.hasNext() ){
				String columnName = columnsNamesIter.next();
                                if (this.RESERVE_WORDS.contains(columnName)) {
                         		columnName = "autochanged_"+columnName;
                                }
				ddl.append(columnName+" STRING,") ;
			}
                        ddl.deleteCharAt(ddl.length() -1 );
			ddl.append(") " ) ;

			ddl.append("PARTITIONED BY (parti STRING) ; \n\n") ;
		}
                ddlout.println(ddl.toString());
		ddlout.flush();
		ddlout.close();
                //println(outddl) ;

        }
	/** simple util method to get all table name */
        private List<String> getTableNames()throws Exception{
		DatabaseMetaData metadata = this.CONN.getMetaData();
		String[] type =  new String[1];
		type[0]= "TABLE";
		List<String> tableNameList = new ArrayList<String>();
		
		ResultSet metadataRs = metadata.getTables(null,null,null,type) ;
		while(metadataRs.next()){
			String tableName = metadataRs.getString(3);
			tableNameList.add(tableName);
		}
		return tableNameList ;
	}


        /** simple util method to get all column names */
        private List<String> getColumnNames(String tableName)throws Exception{
                DatabaseMetaData metadata = this.CONN.getMetaData();
                String[] type =  new String[1];
		ResultSet metadataColumns 
			= metadata.getColumns(null,null,tableName,null);

		List<String> tableNameList = new ArrayList<String>();

                while(metadataColumns.next()){
                	String columnName = metadataColumns.getString(4);
			tableNameList.add(columnName);
		}

		return tableNameList ;

	}
	/** assume that the table name +".data" is the output file name.*/
        public void exportImportCliToFolder(String folderName,String parti,String outputFileName)throws Exception{

        	PrintWriter cliout =
                             new PrintWriter(
                                new BufferedWriter(
                                        new FileWriter(outputFileName)
                                )
                             );


		String loadLocal = "LOAD DATA LOCAL INPATH \"%s/%s.data\" INTO TABLE TALENT PARTITION(parti='%s') ;";
		if(parti == null ){ parti="noPartition"; }

		List<String> tableNames = this.getTableNames();
  		for(String tableName  : tableNames) {
      			cliout.println(String.format(loadLocal,folderName,tableName,parti)); 
    		}
		cliout.flush();
		cliout.close();

	}
	public void exportToFolder(String folderName)throws Exception{
		DatabaseMetaData metadata = this.CONN.getMetaData();
		String[] type =  new String[1];
		type[0]= "TABLE";
		
		ResultSet metadataRs = metadata.getTables(null,null,null,type) ;
		// println("show table/columns...");
		while(metadataRs.next()){
			
			String tableName = metadataRs.getString(3);
			println("export table:"+tableName);
 			PrintWriter tableFileout = 
   			     new PrintWriter(
				new BufferedWriter(
					new FileWriter(folderName+"/"+tableName+".data")
			     	)
                             );
			ResultSet metadataColumns = metadata.getColumns(null,null,tableName,null);

			while(metadataColumns.next()){
				String columnName = metadataColumns.getString(4);
				// println(tableName+"."+columnName);
			 
			}
			
			
			Statement stat = this.CONN.createStatement();
			ResultSet dataSet = stat.executeQuery("select * from "+tableName);
			ResultSetMetaData rsmd = dataSet.getMetaData();
		    int numberOfColumns = rsmd.getColumnCount();
	           while(dataSet.next()){
	        	StringBuffer oneRow = new StringBuffer();
	        	for(int i=1; i<= numberOfColumns ;i++){
	        		oneRow.append(dataSet.getString(i));
	        		oneRow.append(this.SEP);
	        	}
	        	// println(oneRow.toString());
                        tableFileout.println(oneRow.toString());
                        tableFileout.flush();
	        	
	            }
                    tableFileout.close();
          
		}
		
		
	    
	}
    

}
