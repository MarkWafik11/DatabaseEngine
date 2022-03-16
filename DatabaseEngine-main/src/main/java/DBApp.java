import java.io.*;
import java.util.*;

public class DBApp implements DBAppInterface {


	@Override
	public void init() {
		BufferedReader fileReader = null;
		try {
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		} catch (FileNotFoundException e) {
			try {
				FileWriter metadata = new FileWriter("src/main/resources/metadata.csv",true);
				try {
					metadata.write("Table Name,");
					metadata.write("Column Name,");
					metadata.write("Column Type,");
					metadata.write("ClusteringKey,");
					metadata.write("Indexed,");
					metadata.write("min,");
					metadata.write("max" + "\n");
					metadata.flush();
				} catch (IOException eee) {
					eee.printStackTrace();
				}
			} catch (Exception ee) {
				System.out.println("Error in CsvFileWriter !!!");
				ee.printStackTrace();
			}
		}
	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
							Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {

		Vector<String> existingTables = Helpers.getDistinctTableNames();
		//Checking if the table already exists
		if(existingTables.contains(tableName))
			return;
		//Creating new table and adding its columns in metadata

		Table t = new Table(tableName,clusteringKey);

		Set<String> keys = colNameType.keySet();
		FileWriter metadata = new FileWriter("src/main/resources/metadata.csv",true);
		for (String key : keys) {
			try {
				metadata.write(tableName + ",");
				metadata.write(key + ",");
				metadata.write(colNameType.get(key) + ",");
				if (key.equals(clusteringKey))
					metadata.write("True,");
				else
					metadata.write("False,");
				metadata.write("False,");
				metadata.write(colNameMin.get(key) + ",");
				metadata.write(colNameMax.get(key) + "\n");
				metadata.flush();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		metadata.flush();
		Helpers.serialize(t,tableName);
	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// TODO Auto-generated method stub
		Vector<String> existingTables = Helpers.getDistinctTableNames();
		if(! existingTables.contains(tableName))
			throw new DBAppException("The table does not exist");


		Table myTable = (Table) Helpers.deserialize(tableName);
		String clusterKey = myTable.ClusteringKey;
		int indexDimension = 0;

		Hashtable<String ,String > colNames = Helpers.getMetaData(myTable.name);
		ArrayList<String> colNamesArray = new ArrayList<>();
		Enumeration<String> enumeration = colNames.keys();

		while (enumeration.hasMoreElements()){
			colNamesArray.add(enumeration.nextElement());
		}
		for(String colName : columnNames){
			if(colNamesArray.contains(colName)){
				indexDimension++;
			}
			else{
				throw new DBAppException("The column does not exist");
			}
		}
		Grid grid;
		if(!(myTable.tableGrids == null)) {
			System.out.println(myTable.tableGrids);
			if (!(myTable.tableGrids.get(columnNames) == null)) {
				throw new DBAppException("This Grid already exists");
			}
		}
		grid = new Grid(indexDimension, myTable.name , columnNames,myTable.name+"Grid"+ myTable.GridCounter);
		myTable.tableGrids.put(columnNames,grid.name);
		System.out.println(myTable.tableGrids);
		myTable.GridCounter++;


		if(myTable.positions.size() != 0){
			grid.populateGrid(myTable);
		}

		Helpers.serialize(grid,grid.name);
		Helpers.rewriteMetaData(tableName,columnNames);
		Helpers.serialize(myTable,tableName);


	}


	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		Vector<String> existingTables = Helpers.getDistinctTableNames();

		//check if the table does not exist
		if(! existingTables.contains(tableName))
			throw new DBAppException("The table does not exist");

		Table myTable = (Table) Helpers.deserialize(tableName);

		String clusterKey = myTable.ClusteringKey;


		//checking that columns already exist
		Hashtable<String ,String > colNames = Helpers.getMetaData(myTable.name);
		ArrayList<String> colNamesArray = new ArrayList<>();
		Enumeration<String> enumeration = colNames.keys();
		while (enumeration.hasMoreElements()){
			colNamesArray.add(enumeration.nextElement());
		}

		if (clusterKey != null && (!(colNameValue.containsKey(clusterKey) || myTable == null))) {
			throw new DBAppException();
		}


		enumeration = colNameValue.keys();
		Vector<String> Keys = new Vector<>();

		while(enumeration.hasMoreElements()){
			String element = enumeration.nextElement();
			if(!colNamesArray.contains(element))
				throw new DBAppException();
			Keys.add(element);
		}
		if(!Keys.contains(clusterKey))
			throw new DBAppException();

		//Check the type and if the value within the range

		while (enumeration.hasMoreElements()) {
			String colName = enumeration.nextElement();
			Object value = colNameValue.get(colName);

			Hashtable <String,String> checkers2 = Helpers.getMetaData(myTable.name);
			Hashtable<String,Vector<String>> minMaxChecker = Helpers.getMetaData3(myTable.name);
			if (!checkers2.containsKey(colName))
				throw new DBAppException();
			else {
				Helpers.CheckType(checkers2,colName,value);
				boolean flag = Helpers.CheckMinandMax(minMaxChecker,colName,value);
				if(!flag)
					throw new DBAppException();
			}
		}

		myTable.insertIntoPages(colNameValue, tableName);
		Helpers.serialize(myTable,tableName);

	}


	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		Table myTable = (Table) Helpers.deserialize(tableName);
		if(myTable == null){
			throw new DBAppException("This table does not exist");
		}

		Hashtable<String ,String > colNames = Helpers.getMetaData(myTable.name);
		ArrayList<String> colNamesArray = new ArrayList<>();
		Enumeration<String> enumeration = colNames.keys();
		while (enumeration.hasMoreElements()){
			colNamesArray.add(enumeration.nextElement());
		}

		Enumeration<String> enumeration3 = columnNameValue.keys();
		while (enumeration3.hasMoreElements()){
			String colName = enumeration3.nextElement();
			Object value = columnNameValue.get(colName);
			Hashtable <String,String> checkers2 = Helpers.getMetaData(myTable.name);
			Hashtable<String,Vector<String>> minMaxChecker = Helpers.getMetaData3(myTable.name);
			if (!checkers2.containsKey(colName))
				throw new DBAppException();
			else {
				Helpers.CheckType(checkers2,colName,value);
				boolean flag = Helpers.CheckMinandMax(minMaxChecker,colName,value);
				if(!flag)
					throw new DBAppException("Out of limit bounds or type is not correct");
			}
		}
		Enumeration<String> enumeration2 =  columnNameValue.keys();
		Vector<String> keys = new Vector<>();

		while(enumeration2.hasMoreElements()){
			String element = enumeration2.nextElement();
			if(!colNamesArray.contains(element))
				throw new DBAppException();
			keys.add(element);
		}  // check that clustering key is not in the update hashtable given
		if(keys.contains(myTable.ClusteringKey))
			throw new DBAppException();


		if (myTable == null)
			throw new DBAppException();
		else{
			enumeration = columnNameValue.keys();
			while(enumeration.hasMoreElements()){
				String key = enumeration.nextElement();
				if (key.equals(myTable.ClusteringKey))
					throw new DBAppException();
				else if(!colNamesArray.contains(key))
					throw new DBAppException();
				else
					myTable.UpdatePages(columnNameValue, clusteringKeyValue,myTable);
			}
		}

		Helpers.serialize(myTable,tableName);
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		Table myTable = (Table) Helpers.deserialize(tableName);
		if(myTable == null)
			throw new DBAppException("The Table you are trying to Delete from does not exist.");
		else
			myTable.deleteFromPages(columnNameValue, tableName);
		Helpers.serialize(myTable,tableName);
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		String strTableName = sqlTerms[0].get_strTableName();
		if(!((sqlTerms[0]._strTableName).equals(sqlTerms[1]._strTableName))){
			throw new DBAppException("Table names are not the same");
		}
		Table t = Helpers.deserialize(strTableName);
		if (sqlTerms.length != arrayOperators.length + 1)
			throw new DBAppException("Size of Operators must match size of columns");
		Iterator it;
		if(t.tableGrids != null){
			 it = t.selectWithIndex(sqlTerms,arrayOperators);
		}
		else {
			it = t.selectFromTableWithoutIndex(sqlTerms, arrayOperators);
		}
		while(it.hasNext()){
			Record r = (Record) it.next();
			System.out.println(r.values);
		}

		return it;
	}
}
