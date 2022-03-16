import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;


public class Table implements Serializable{

	private static final long serialVersionUID = 1L;
	String name;
	String ClusteringKey;
	Vector<String> positions;  // this will have the position of pages in order e.g. <p2,p1,p3,p4,p5> each time we create
								 // new pages we must update positions to be sorted using its minimum value
	Hashtable <String, ArrayList<Object>> MinMaxSize; //p2 --> <1,2,3>
	int Counter;
	int GridCounter;
	Hashtable<String[],String> tableGrids; //colNames ----> grid name

	public Table(String name,String ClusteringKey) {
		this.name = name;
		Counter = 1; //This is the pages counter
		this.GridCounter = 1 ; // this is the grid counter grid name should look like StudentsGrid1
		this.ClusteringKey = ClusteringKey;
		this.tableGrids = new Hashtable<String[],String>();
		positions = new Vector<String>();
		MinMaxSize = new Hashtable<String,ArrayList<Object>>();
	}

	public void UpdatePages(Hashtable ht , String ClusteringKey,Table t) {
	String check = Helpers.getclusterkeyType(t.name);
	Object x = ClusteringKey;
		if(check.equals("java.lang.Integer")){
			x = Integer.parseInt(ClusteringKey);
		}
		else if(check.equals("java.lang.Double")){
			x = Double.parseDouble(ClusteringKey);
		}
		else if(check.equals("java.util.Date")){
			try {
				x = new SimpleDateFormat("YYYY-MM-DD").parse(ClusteringKey);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	int low = 0;
	int high = positions.size()-1;
	int mid = (low + high) / 2;

	while(low <= high) {
		mid = (high + low) / 2;
		if(mid == positions.size()){
			mid--;
			break;
		}
		String ofMid = positions.get(mid);
		Object min = MinMaxSize.get(ofMid).get(0);
		if(Helpers.RangeWithinthepage(x,min,MinMaxSize.get(ofMid).get(1))){
			break;
		}
		else if (Helpers.XlessthanY(x, MinMaxSize.get(ofMid).get(0))) {
			high = mid - 1;
		} else if (Helpers.XlessthanY(MinMaxSize.get(ofMid).get(1), x)) {
			low = mid + 1;
		} else {
			break;
		}
	} //Binary Search to get the mid

	String pageName = positions.get(mid);
	Page p = deserializePage(pageName);

	//x = Helpers.getclusterkeyValue(ht, TableName);
	low = 0 ;
	high = p.size()-1;
	mid = (low + high) / 2;
	Record r = null;
	while(low <= high) {
		mid = (high + low) / 2;
		if(mid == p.records.size()){
			mid--;
			break;
		}
		if(Helpers.XequalY(Helpers.getclusterkeyValue2(p.records.get(mid),t.name),x)){
			r = p.records.get(mid);
			break;
		}
		else if(Helpers.XlessthanY(Helpers.getclusterkeyValue2(p.records.get(mid),t.name),x)) {
			low = mid + 1;
		} else if (Helpers.XlessthanY(x,Helpers.getclusterkeyValue2(p.records.get(mid),t.name))) {
			high = mid - 1;
		} else {
			break;
		}
	} //Binary Search to get the record

	Enumeration<String> enumeration = ht.keys();
	//Object clusterKey1 = Helpers.getclusterkeyValue(ht, TableName);

	if(r != null) {
		while(enumeration.hasMoreElements()) {
			String key = enumeration.nextElement();
			int colIndex = p.columns.indexOf(key);
			p.records.get(mid).values.set(colIndex,ht.get(key));
		}
	}
	serializePage(p);
	if(this.tableGrids != null){
		updateBucket(ht,pageName,t, ClusteringKey);
	}

	}
	//age = 30
	public void updateBucket(Hashtable ht, String pageName,Table table,String clusterKey) {
		Enumeration<String[]> enumeration = tableGrids.keys();
		Enumeration<String> enumeration2 = ht.keys();
		while(enumeration.hasMoreElements()){ //Loops on grids .. get each grid and then extract the indexingcols
			String [] gridCols = enumeration.nextElement();
			enumeration2 = ht.keys();
			while(enumeration2.hasMoreElements()){
				if(Helpers.includes(gridCols,enumeration2.nextElement())){
					Grid g = deserializeGrid(tableGrids.get(gridCols));
					Page p  = deserializePage(pageName);
					for(int i = 0 ; i < p.records.size() ;  i++)
						if(Helpers.XequalY(p.records.get(i).clusterkey,clusterKey)){
							Vector<Object> values = new Vector<>();
							for(int j = 0 ; j < gridCols.length ; j++){
								values.add(p.records.get(i).values.get(p.columns.indexOf(gridCols[j])));
							}
							g.insertIntoBucket(values,pageName, table);

						}

				}
			}
		}

	}

	public void deleteFromPages(Hashtable ht , String TableName) { //throws exception ???????????????????
		Boolean delete = true;
		if(!(ht.get(ClusteringKey)==null)){
			Object x = Helpers.getclusterkeyValue(ht,TableName);
			int low = 0;
			int high = positions.size()-1;
			int mid = (low + high) / 2;

			while(low <= high) {
				mid = (high + low) / 2;
				if(mid == positions.size()){
					mid--;
					break;
				}
				String ofMid = positions.get(mid);
				Object min = MinMaxSize.get(ofMid).get(0);
				if(Helpers.RangeWithinthepage(x,min,MinMaxSize.get(ofMid).get(1))){
					break;
				}
				else if (Helpers.XlessthanY(x, MinMaxSize.get(ofMid).get(0))) {
					high = mid - 1;
				} else if (Helpers.XlessthanY(MinMaxSize.get(ofMid).get(1), x)) {
					low = mid + 1;
				} else {
					break;
				}
			} //Binary Search to get the mid
			String pageName = positions.get(mid);
			Page p = deserializePage(pageName);
			low = 0 ;
			high = p.size()-1;
			int mid2 = (low + high) / 2;
			Record r = null;
			while(low <= high) {
				mid2 = (high + low) / 2;
				if(mid2 == p.records.size()){
					mid2--;
					break;
				}
				if(Helpers.XequalY(Helpers.getclusterkeyValue2(p.records.get(mid2),TableName),x)){
					r = p.records.get(mid2);
					break;
				}
				else if(Helpers.XlessthanY(Helpers.getclusterkeyValue2(p.records.get(mid2),TableName),x)) {
					low = mid2 + 1;
				} else if (Helpers.XlessthanY(x,Helpers.getclusterkeyValue2(p.records.get(mid2),TableName))) {
					high = mid2 - 1;
				} else {
					break;
				}
			} //Binary Search to get the record
			Enumeration<String> enumeration = ht.keys();
			while(enumeration.hasMoreElements()){
				String element = enumeration.nextElement();
				if(!element.equals(ClusteringKey)){
					int index = p.columns.indexOf(element);
					delete = delete & Helpers.XequalY(p.records.get(mid2).values.get(index), ht.get(element)) ;
				}
			}
			if(delete){
				p.records.remove(mid2);
			}
			if(p.isEmpty()){
				positions.remove(mid);
				MinMaxSize.remove(p.PageName);
				File file = new File("src/main/resources/data/"+p.PageName+".ser");
				file.delete();
			}
			else{
				serializePage(p);
			}
		}
		else{
			Iterator<String> iter = positions.iterator();

			while (iter.hasNext()){
				String page = iter.next();
				Page p = deserializePage(page);
				Iterator<Record> iter2 = p.records.iterator();
				while(iter2.hasNext()){
					Record record = iter2.next();
					delete = true;
					Enumeration<String> enumeration = ht.keys();
					while(enumeration.hasMoreElements()){
						String element = enumeration.nextElement();
						int index = p.columns.indexOf(element);
						delete = delete & Helpers.XequalY(record.values.get(index), ht.get(element)) ;
					}
					if(delete) {
						//p.records.remove(record);
						iter2.remove();
					}
				}
				if(p.isEmpty()){
					//positions.remove(page);
					MinMaxSize.remove(page);
					iter.remove();
					File file = new File("src/main/resources/data/"+page+".ser");
					file.delete();
				}
				else{
					serializePage(p);
				}
			}
		}
		deleteFromGrid(ht);
	}

	public void deleteFromGrid(Hashtable ColNameValues){ // age,gpa ---> 20    G1(gpa)         G2(age,gpa)
		Enumeration<String[]> gridArr = tableGrids.keys();
		Vector<Object> values = new Vector<Object>();
		boolean flag = true;
		while (gridArr.hasMoreElements()){
			String[] indexingCols = gridArr.nextElement();
			Enumeration<String> Hashtablekeys = ColNameValues.keys();
			while(Hashtablekeys.hasMoreElements()){
				flag = true;
				if(!Helpers.includes(indexingCols,Hashtablekeys.nextElement())){
					flag = false;
				}
			}
			if(flag){
				Grid g = deserializeGrid(tableGrids.get(indexingCols));
				g.deleteFromBucket(ColNameValues, this);
				serialize(g,g.name);

			}
		}
	}

	public HashSet XOR(HashSet<String> hs1 , HashSet<String> hs2){
		HashSet<String> result = new HashSet<>();
		for(String s : hs1){
			if(!(hs2.contains(s)))
				result.add(s);
		}
		for(String s : hs2){
			if(!(hs1.contains(s)))
				result.add(s);
		}

		return result;
	}


	public HashSet OR(HashSet<String> hs1 , HashSet<String> hs2){
		HashSet<String> result = new HashSet<>();
		for(String s : hs1){
			result.add(s);
		}
		for(String s : hs2){
			result.add(s);
		}

		return result;
	}

	public HashSet AND(HashSet<String> hs1 , HashSet<String> hs2){
		HashSet<String> result = new HashSet<>();
		for(String s : hs1){
			if(hs2.contains(s)){
				result.add(s);
			}
		}


		return result;
	}



	public HashSet Equal(String colName , Object val, Grid g){
		String [] cols = g.colNames;
		double [] ranges = g.range;
		HashSet<String> bucketsIncluded = new HashSet<>();
		double range = 0.0;
		int i = 0;
		for(i = 0 ; i < cols.length ; i++){
			if(cols[i].equals(colName)){
				range = ranges[i];
				break;
			}
		}
		double value = Helpers.getVal(val);
		int factor = (i==0)? 1:i*10;
		int StartingIndex = (int) (factor * (Math.floor(Math.abs(value - Helpers.getMinimumValue(g.name,name)) / (range))));

		if(factor == 1){
			for(i = StartingIndex ; i <g.index.length ; i+=10){
				bucketsIncluded.add(g.index[i]);
			}
		}
		else{
			for(i = StartingIndex ; i < g.index.length ; i+=factor) {
				for (int j = StartingIndex; j < StartingIndex + 10; j++) {
					bucketsIncluded.add(g.index[i]);
				}
			}
		}
		return bucketsIncluded;
	}
	public HashSet smallerThan(String colName , Object val, Grid g){
		String [] cols = g.colNames;
		double [] ranges = g.range;
		HashSet<String> bucketsIncluded = new HashSet<>();
		double range = 0.0;
		int i = 0;
		for(i = 0 ; i < cols.length ; i++){
			if(cols[i].equals(colName)){
				range = ranges[i];
				break;
			}
		}
		double value = Helpers.getVal(val);
		int factor = (i==0)? 1:i*10;
		int StartingIndex = (int) (factor * (Math.floor(Math.abs(value - Helpers.getMinimumValue(g.name,name)) / (range))));

		if(factor == 1){
			for(i = 0 ; i < g.index.length ; i+=10,StartingIndex+=10){
				for(int j = i ; j <= StartingIndex ; j++){
					bucketsIncluded.add(g.index[j]);
				}
			}
		}
		else{
			for(i = 0 ;  i < g.index.length; i += factor, StartingIndex += factor) {
				for (int j = i ; j <= StartingIndex; j++) {
					bucketsIncluded.add(g.index[j]);
				}
			}
		}
		return bucketsIncluded;
	}

	public HashSet NotEqual(Grid grid){
		HashSet hs = new HashSet();
		hs.addAll(Arrays.asList(grid.index));
		return hs;
	}

	public HashSet greaterThan(String colName , Object val, Grid g){
		String [] cols = g.colNames; //[gpa, name , id]
		double [] ranges = g.range;
		HashSet<String> bucketsIncluded = new HashSet<>();
		double range = 0.0;
		int i = 0;
		for(i = 0 ; i < cols.length ; i++){
			if(cols[i].equals(colName)){
				range = ranges[i];
				break;
			}
		}
		double value = Helpers.getVal(val);
		int factor = (i==0)? 1:(int) Math.pow(10,i);
		int StartingIndex = (int) (factor * (Math.floor(Math.abs(value - Helpers.getMinimumValue(g.name,name)) / (range))));


		if(factor == 1){
			int nine = 9;
			for(i = StartingIndex ; i < g.index.length ; nine+=10,  i+= 10) {
				for(int j = i; j <= nine ; j++) {
					bucketsIncluded.add(g.index[j]);
				}
			}
		}
		else{
			int n = ( factor*10 ) - 1;
			for(i = StartingIndex ; i < g.index.length ; i+=factor){
				for(int j = i ; j <= n ; j++){
					bucketsIncluded.add(g.index[j]);
				}
				n+= factor*10;
			}

		}
//		for(i = StartingIndex ; i <g.index.length ; i+=(int) Math.pow(10,cols.length-1)){
//			for(int j = i ; j < factor * ((i / factor)+1) ; j++){
//				bucketsIncluded.add(g.index[j]);
//			}
//		}
		return bucketsIncluded;
	}





	public void insertIntoGrid(Hashtable ColNameValues , String pageAddress){
		Enumeration<String[]> enumeration = tableGrids.keys();
		Vector<Object> values = new Vector<Object>();
		while (enumeration.hasMoreElements()){
			String[] indexingCols = enumeration.nextElement();
			for(String element: indexingCols){
				values.add(ColNameValues.get(element));
			}
			Grid g = deserializeGrid(tableGrids.get(indexingCols));
			g.insertIntoBucket(values,pageAddress,this);
			serialize(g,g.name);

		}

	}

	public static String[] getColumnNamesFromSQLTerm(SQLTerm[] arrSQLTerms){
		String [] tmp = new String[arrSQLTerms.length];
		for (int i=0;i<arrSQLTerms.length;i++) {
			tmp[i]=arrSQLTerms[i]._strColumnName;
		}
		return tmp;
	}



	public void insertIntoPages(Hashtable ht , String TableName) {
		if(positions.isEmpty()) {
			Page p = new Page(TableName + Counter , TableName);
			positions.add(TableName + Counter);
			p.insertIntoPage(ht, TableName);
			if(this.tableGrids != null)
				insertIntoGrid(ht,p.PageName);
			ArrayList<Object> MinMaxSizeArray = new ArrayList<>();
			MinMaxSizeArray.add(p.getminValue());
			System.out.println(p.getminValue());
			MinMaxSizeArray.add(p.getmaxValue());
			MinMaxSizeArray.add(p.size());
			MinMaxSize.put(p.PageName, MinMaxSizeArray);
			Counter++;
			serializePage(p);
			//p.CreateSerializedPaged();
			//pages.add(p);
			return;
		}

		Object x = Helpers.getclusterkeyValue(ht, TableName);
		int low = 0;
		int high = positions.size()-1;
		int mid = (low + high) / 2;
		while(low <= high) {
			mid = (high + low) / 2;
			if(mid == positions.size()){
				mid--;
				break;
			}
			String ofMid = positions.get(mid);
			if(Helpers.RangeWithinthepage(x,MinMaxSize.get(ofMid).get(0),MinMaxSize.get(ofMid).get(1))){
				break;
			}
			else if (Helpers.XlessthanY(x, MinMaxSize.get(ofMid).get(0))) {
				high = mid - 1;
			} else if (Helpers.XlessthanY(MinMaxSize.get(ofMid).get(1), x)) {
				low = mid + 1;
			} else {
				break;
			}
		} //Binary Search to get the mid

		String pageName = positions.get(mid); // getting page name and deserialize it
		Page MidPage = deserializePage(pageName);

		if(Helpers.XlessthanY(MinMaxSize.get(pageName).get(2) , MidPage.maxSize)){ //if the page is not full
			//Page newPage = pages.get(pages.indexOf( positions.get(mid))).GetSerializedPaged();
			MidPage.insertIntoPage(ht, TableName);
			if(this.tableGrids != null)
				insertIntoGrid(ht,MidPage.PageName);
			ArrayList<Object> MinMaxSizeArray = new ArrayList<>(); //Update the min and max value of the page already existed
			MinMaxSizeArray.add(MidPage.getminValue());
			MinMaxSizeArray.add(MidPage.getmaxValue());
			MinMaxSizeArray.add(MidPage.records.size());
			MinMaxSize.put(MidPage.PageName, MinMaxSizeArray);
			serializePage(MidPage);
			//newPage.CreateSerializedPaged();
		}
		else{
			Page newPage = new Page(TableName + Counter , TableName);
			Counter++;
			//Page oldPage = pages.get(pages.indexOf(positions.get(mid))).GetSerializedPaged();
			//Page oldPage = deserializePage(pageName);
			int j=0;
			for(int i = MidPage.records.size() / 2 ; i < MidPage.records.size() ; i++){
				newPage.records.add(MidPage.records.get(i));
				j++;
				//MidPage.records.remove(MidPage.records.get(i));
			}
			while(j != 0){
				MidPage.records.remove(MidPage.records.lastElement());
				j--;
			}

			java.util.Collections.sort(newPage.records);
			if(Helpers.XlessthanY(x,newPage.getminValue())){
				MidPage.insertIntoPage(ht,TableName);
				if(this.tableGrids != null)
					insertIntoGrid(ht,MidPage.PageName);
			}
			else{
				newPage.insertIntoPage(ht,TableName);
				if(this.tableGrids != null)
					insertIntoGrid(ht,newPage.PageName);
			}

			ArrayList<Object> newArray = new ArrayList<Object>(3);
			newArray.add(MidPage.getminValue());
			newArray.add(MidPage.getmaxValue());
			newArray.add(MidPage.records.size());
			MinMaxSize.put(MidPage.PageName,newArray);
			newArray = new ArrayList<>();
			newArray.add(newPage.getminValue());
			newArray.add(newPage.getmaxValue());
			newArray.add(newPage.records.size());
			MinMaxSize.put(newPage.PageName,newArray);
			positions.add(mid+1,newPage.PageName);
			serializePage(MidPage);
			serializePage(newPage);

		}
	}
	public void serialize(Grid grid,String tableName){
		FileOutputStream fileOut;
		ObjectOutputStream out;
		String path = "src/main/resources/data/" + tableName + ".ser";
		try {
			fileOut = new FileOutputStream( path);
			out = new ObjectOutputStream(fileOut);
			out.writeObject(grid);
			out.close();
			fileOut.close();
			System.out.println("Grid is Serialized");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	public void serializePage(Page p) {
		String path = "src/main/resources/data/" + p.PageName + ".ser";
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();
			System.out.println("Page " + p.PageName + " is successfully serialized in " + path);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public Page deserializePage(String pageName) {
		String path = "src/main/resources/data/" + pageName + ".ser";
		FileInputStream fileIn;
		try {
			fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page p = (Page) in.readObject();
			in.close();
			fileIn.close();
			return p;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}
		return null;
	}
	public Grid deserializeGrid(String GridName) {
		FileInputStream fileIn;
		String path = "src/main/resources/data/" + GridName + ".ser";
		try {
			fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			System.out.println("Grid " + GridName + " is successfully deserialized in " + path);
			return (Grid) in.readObject();

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}

		return null;
	}
	private boolean checkType(Object value, String type) {
		String Type = (value.getClass().toString().split(" "))[1].trim();
		return Type.equals(type);
	}
	private static Comparable getComparable(Object value, String type) {
		Comparable c = null;
		if (type.equals("java.lang.Integer")) {
			c = (Integer) value;
		} else if (type.equals("java.lang.String")) {
			c = (String) value;
		} else if (type.equals("java.lang.Double")) {
			c = (Double) value;
		}  else if (type.equals("java.util.Date")) {
			c = (java.util.Date) value;
		} else if (type.equals("java.lang.Boolean")) {
			c = (java.lang.Boolean) value;
		}
		return c;
	}

	public boolean checkTerm(Record r, SQLTerm sqlTerm) throws DBAppException {

		String colName = sqlTerm.get_strColumnName();
		Comparable val = (Comparable) sqlTerm.get_objValue();

        Hashtable<String,String> v=Helpers.getMetaData(this.name);
		if (!v.containsKey(colName)) {
			throw new DBAppException("Invalid column name");
		}
		String type = v.get(colName);

		if (!checkType(val,type)) {
			throw new DBAppException("Invalid column type");
		}
		int colIndex = Helpers.getMetaData2(this.name).indexOf(colName);


		Comparable recordVal = getComparable(r.values.get(colIndex), type);

		String op = sqlTerm.get_strOperator();

		switch (op) {
			case "=":
				return recordVal.equals(val);
			case ">":
				return recordVal.compareTo(val) > 0;
			case ">=":
				return recordVal.compareTo(val) >= 0;
			case "<":
				return recordVal.compareTo(val) < 0;
			case "<=":
				return recordVal.compareTo(val) <= 0;
			case "!=":
				return !recordVal.equals(val);

			default : throw new DBAppException("Invalid operator");

		}


	}


	public Iterator selectWithIndex(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException{
		Vector <HashSet> result = new Vector<HashSet>();
		String [] ColumnNames = getColumnNamesFromSQLTerm(sqlTerms);
		if(Helpers.CheckHashtableContainslist(tableGrids, ColumnNames)){
			String gridName = tableGrids.get(ColumnNames);
			Grid g = deserializeGrid(gridName);
			for(int i = 0 ; i < sqlTerms.length; i++){
				Object val = sqlTerms[i]._objValue;
				String operator = sqlTerms[i]._strOperator;
				String colName = sqlTerms[i]._strColumnName;
				switch(operator){
					case "=":
						result.add(Equal( colName , val, g)); break;
					case ">":
						result.add(greaterThan( colName , val, g)); break;
					case ">=":
						result.add(greaterThan( colName , val, g)); break;
					case "<":
						result.add(smallerThan( colName , val, g)); break;
					case "<=":
						result.add(smallerThan( colName , val, g)); break;
					case "!=":
						result.add(NotEqual(g)); break;
				}
			}
		}
		else{
			boolean flag = true;
			for(String colName : ColumnNames){
				if(!(tableGrids.contains(colName))) {
					flag = false;
					break;
				}
			}

			if(!flag){
				return selectFromTableWithoutIndex(sqlTerms,arrayOperators);
			}
			else{
				for(int i = 0 ; i < sqlTerms.length; i++){
					String gridName = tableGrids.get(sqlTerms[i]._strColumnName);
					Grid g = deserializeGrid(gridName);
					Object val = sqlTerms[i]._objValue;
					String operator = sqlTerms[i]._strOperator;
					String colName = sqlTerms[i]._strColumnName;
					switch(operator){
						case "=":
							result.add(Equal( colName , val, g)); break;
						case ">":
							result.add(greaterThan( colName , val, g)); break;
						case ">=":
							result.add(greaterThan( colName , val, g)); break;
						case "<":
							result.add(smallerThan( colName , val, g)); break;
						case "<=":
							result.add(smallerThan( colName , val, g)); break;
						case "!=":
							result.add(NotEqual(g)); break;
					}
				}
			}


		}
		for(int i = 0 ; i < arrayOperators.length ; i++) {
			switch (arrayOperators[i]) {
				case "AND":
					result.add(i + 1, AND(result.get(i), result.get(i + 1)));
					break;
				case "OR":
					result.add(i + 1, OR(result.get(i), result.get(i + 1)));
					break;
				case "XOR":
					result.add(i + 1, XOR(result.get(i), result.get(i + 1)));
					break;
				default:
					throw new DBAppException("Invalid connector !");
			}
		}

		HashSet<String> pages = Helpers.getPagesFromBucket(result.get(result.size()-1));
		Vector<Record> result2 = new Vector<Record>();

		for(String page : pages){
			Page p = deserializePage(page);
			for (int j = 0; j < p.size(); j++) {
				Record record = p.get(j);
				if (CheckCondition(record, sqlTerms, arrayOperators)) {
					result2.add(record);
				}
			}
		}
		return result2.iterator();

	}

	public boolean CheckCondition(Record r, SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws  DBAppException {

		if (arrSQLTerms.length == 0)
			return true;

		boolean result = checkTerm(r, arrSQLTerms[0]);

		for (int i = 1; i < arrSQLTerms.length; i++) {
			boolean currTermEval = checkTerm(r, arrSQLTerms[i]);

			switch (strarrOperators[i - 1].toUpperCase().trim()) {
				case "AND":
					result &= currTermEval;
					break;
				case "OR":
					result |= currTermEval;
					break;
				case "XOR":
					result ^= currTermEval;
					break;

				default:
					throw new DBAppException("Invalid connector !");
			}

		}

		return result;

	}


	public Iterator selectFromTableWithoutIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws  DBAppException {
		Vector<Record> result = new Vector<Record>();

		for (int i = 0; i < positions.size(); i++) {
			Page p = deserializePage(positions.get(i));
			for (int j = 0; j < p.size(); j++) {
				Record record = p.get(j);
				if (CheckCondition(record, arrSQLTerms, strarrOperators)) {
					result.add(record);
				}
			}
		}
		return result.iterator();
	}

	public static void main(String[] args) throws DBAppException {

		String [] x ={"1","2","3"};
		String [] y ={"1","9","3"};
		Boolean flag =true;
		for(String element:x){
			if (!Helpers.includes(y,element)){
				flag=false;
				System.out.print(flag);
			}}
		System.out.print(flag);;
	}

}
