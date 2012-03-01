package org.bigradzebra.experience;
import java.sql.*;

public class RDBExporter {

	private static String SEP="@@@";
	private Connection CONN ;
	/**
	 * @param args
	 */
	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		println("export RDB data");
		RDBExporter exporter = new RDBExporter("test.db");
		exporter.exportToFolder("nosuchfolder");
	}
	public static void println(Object msg){
		System.out.println(msg);
	}

	public RDBExporter(String dbname)throws Exception{
		Class.forName("org.sqlite.JDBC");	
		this.CONN =
	    	      DriverManager.getConnection("jdbc:sqlite:"+dbname);

	}
	public void exportToFolder(String folderName)throws Exception{
		DatabaseMetaData metadata = this.CONN.getMetaData();
		String[] type =  new String[1];
		type[0]= "TABLE";
		
		ResultSet metadataRs = metadata.getTables(null,null,null,type) ;
		println("show table/columns...");
		while(metadataRs.next()){
			String tableName = metadataRs.getString(3);
			ResultSet metadataColumns = metadata.getColumns(null,null,tableName,null);
			while(metadataColumns.next()){
				String columnName = metadataColumns.getString(4);
				println(tableName+"."+columnName);
				Statement stat = this.CONN.createStatement();
		   
		        ResultSet dataSet = stat.executeQuery("select * from "+tableName);
		        
		        while(dataSet.next()){
		        	println(dataSet);
		        }
			}
		}
		
	    
	}
	
    

}
