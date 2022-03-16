import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Helpers {
	public static boolean CheckMinandMax(Hashtable<String, Vector<String>> minMaxChecker, String colName, Object value) {
		Object min = minMaxChecker.get(colName).get(0);
		Object max = minMaxChecker.get(colName).get(1);
		return RangeWithinthepage(value, min, max);

	}

	public static Hashtable<String, String> getMetaData(String tablename) {
		BufferedReader fileReader = null;
		String[] array;
		Hashtable<String, String> ht = new Hashtable<String, String>();
		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				if (array[0].equals(tablename))
					ht.put(array[1], array[2]);
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ht;
	}

	public static Vector<String> getMetaData2(String tablename) {
		Vector<String> temp = new Vector<>();
		BufferedReader fileReader = null;
		String[] array;

		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				if (array[0].equals(tablename))
					temp.add(array[1]);
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return temp;
	}

	public static Hashtable<String, Vector<String>> getMetaData3(String tablename) { //returns each col with its min and max
		Hashtable<String, Vector<String>> temp = new Hashtable<String, Vector<String>>();
		BufferedReader fileReader = null;
		String[] array;

		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				Vector<String> v = new Vector<>();
				array = l.split(",");
				if (array[0].equals(tablename)) {
					v.add(array[5]);
					v.add(array[6]);
					temp.put(array[1], v);
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return temp;
	}

	public static void rewriteMetaData(String tableName, String[] colNames) {
		BufferedReader fileReader = null;
		String[] array;
		StringBuilder sb = new StringBuilder();
		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				if (array[0].equals(tableName)) {
					for (String col : colNames) {
						if (col.equals(array[1]))
							array[4] = "true";
					}
				}
				for (String s : array) {
					sb.append(s + ",");
				}
				sb.append("\n");
			}
			FileWriter metadata = new FileWriter("src/main/resources/metadata.csv", false);
			metadata.write("");
			metadata = new FileWriter("src/main/resources/metadata.csv", true);


			String[] lines = sb.toString().split("\n");
			for (String s : lines) {
				String[] arr = s.split(",");
				for (String element : arr) {
					metadata.write(element + ",");
				}
				metadata.write("\n");
				metadata.flush();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Vector<String> getDistinctTableNames() {
		BufferedReader fileReader = null;
		String[] array;
		Vector<String> v = new Vector<>();
		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			fileReader.readLine();
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				v.add(array[0]);
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return v;
	}


	public static String getclusterkey(String tablename) {
		String s = "";
		BufferedReader fileReader = null;
		String[] array;

		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				if (array[0].equals(tablename) && array[3].equals("True")) {
					s = array[1];
					break;
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return s;
	}

	public static Object getclusterkeyValue(Hashtable ht, String TableName) {
		String s = Helpers.getclusterkey(TableName);
		Object clusterkey = null;
		for (int i = 0; i < Helpers.getMetaData2(TableName).size(); i++) {
			if (Helpers.getMetaData2(TableName).get(i).equals(s)) {
				clusterkey = ht.get(s);
			}
		}
		return clusterkey;
	}

	public static Object getclusterkeyValue2(Record r, String TableName) {
		String s = Helpers.getclusterkey(TableName);
		Object clusterkey = null;
		for (int i = 0; i < Helpers.getMetaData2(TableName).size(); i++) {
			if (Helpers.getMetaData2(TableName).get(i).equals(s)) {
				clusterkey = r.values.get(i);
			}
		}
		return clusterkey;
	}

	public static int getIndexingColumn(String [] colNames, String tableName) {
		int j = 0;
		for (int i = 0; i < colNames.length; i++) {
			if (colNames[i].equals(Helpers.getclusterkey(tableName))) {
				j = i;
				break;
			}
		}
		return j;
	}

	public static void CheckType(Hashtable<String, String> checkers2, String colName, Object value) throws DBAppException {
		if (checkers2.get(colName).equals("java.lang.Integer")) {
			if (!(value instanceof Integer))
				throw new DBAppException();
		} else if (checkers2.get(colName).equals("java.lang.String")) {
			if (!(value instanceof String))
				throw new DBAppException();
		} else if (checkers2.get(colName).equals("java.lang.Double")) {
			if (!(value instanceof Double))
				throw new DBAppException();
		} else if (checkers2.get(colName).equals("java.lang.Boolean")) {
			if (!(value instanceof Boolean))
				throw new DBAppException();
		} else if (checkers2.get(colName).equals("java.util.Date")) {
			if (!(value instanceof java.util.Date))
				throw new DBAppException();
		}

	}

	public static double getMaximumValue(String columnName, String tableName) {
		BufferedReader fileReader = null;
		String[] array;
		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				if (array[0].equals(tableName) && array[1].equals(columnName)) {
					return getValFromString(array[6]);
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0.0;
	}


	public static double getMinimumValue(String columnName, String tableName) {
		BufferedReader fileReader = null;
		String[] array;
		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				if (array[0].equals(tableName) && array[1].equals(columnName)) {
					return getValFromString(array[5]);
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0.0;
	}

	public static boolean includes(String[] arr, String s) {
		for (String s1 : arr) {
			if (s.equals(s1)) {
				return true;
			}

		}
		return false;
	}

	public static boolean XlessthanY(Object x, Object y) {
		System.out.println(x.getClass());
		if (x instanceof Integer)
			if (((Integer) x).compareTo((Integer) y) < 0)
				return true;
		if (x instanceof String)
			if (((String) x).compareTo((String) y) < 0)
				return true;
		if (x instanceof Double)
			if (((Double) x).compareTo((Double) y) < 0)
				return true;

		if (x instanceof Boolean)
			if (((Boolean) x).compareTo((Boolean) y) < 0)
				return true;

		if (x instanceof Date)
			if (((Date) x).compareTo((Date) y) < 0)
				return true;

		return false;
	}

	public static boolean XequalY(Object x, Object y) {
		if (x instanceof Integer)
			if (((Integer) x).compareTo((Integer) y) == 0)
				return true;
		if (x instanceof String)
			if (((String) x).compareTo((String) y) == 0)
				return true;
		if (x instanceof Double)
			if (((Double) x).compareTo((Double) y) == 0)
				return true;

		if (x instanceof Boolean)
			if (((Boolean) x).compareTo((Boolean) y) == 0)
				return true;

		if (x instanceof java.util.Date)
			if (((java.util.Date) x).compareTo((java.util.Date) y) == 0)
				return true;

		return false;
	}

	public static boolean RangeWithinthepage(Object x, Object min, Object max) {
		if (x instanceof Integer) {
			if (!(min instanceof Integer)) {
				min = Integer.parseInt((String) min);
			}
			if (!(max instanceof Integer)) {
				max = Integer.parseInt((String) max);
			}
			if (((Integer) x).compareTo((Integer) max) <= 0 &&
					((Integer) x).compareTo((Integer) min) >= 0)
				return true;
		}
		if (x instanceof String) {
			if (!(min instanceof String)) {
				min += "";
			}
			if (!(max instanceof String)) {
				max += "";
			}
			if (((String) x).compareTo((String) max) <= 0 &&
					((String) x).compareTo((String) min) >= 0)
				return true;
		}
		if (x instanceof Double) {
			if (!(min instanceof Double)) {
				min = Double.parseDouble((String) min);
			}
			if (!(max instanceof Double)) {
				max = Double.parseDouble((String) max);
			}
			if (((Double) x).compareTo((Double) max) <= 0 &&
					((Double) x).compareTo((Double) min) >= 0)
				return true;
		}


		if (x instanceof Boolean)
			if (((Boolean) x).compareTo((Boolean) max) <= 0 &&
					((Boolean) x).compareTo((Boolean) min) >= 0)
				return true;

		if (x instanceof Date) {
			try {
				Date max1 = new SimpleDateFormat("YYYY-MM-DD").parse((String) max);
				Date min1 = new SimpleDateFormat("YYYY-MM-DD").parse((String) min);
				if (((Date) x).compareTo((Date) max1) <= 0 &&
						((Date) x).compareTo((Date) min1) >= 0)
					return true;
			} catch (Exception e) {

			}

		}


		return false;
	}

	public static String getclusterkeyType(String tablename) {
		String s = "";
		BufferedReader fileReader = null;
		String[] array;

		try {
			String l = "";
			fileReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			while ((l = fileReader.readLine()) != null) {
				array = l.split(",");
				if (array[0].equals(tablename) && array[3].equals("True")) {
					s = array[2];
					break;
				}
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return s;
	}
	public static Boolean checkSameList(String[] x , String[] y) {
		Boolean flag =true;
		for(String element:x){
			if (!Helpers.includes(y,element)){
				flag=false;
				return flag;
			}}
		return flag ;
	}

	public static boolean CheckHashtableContainslist(Hashtable<String[], String> tableGrids, String[] y) {
		Enumeration<String[]> enumeration = tableGrids.keys();
		while(enumeration.hasMoreElements()) {
			String [] tmp=enumeration.nextElement();
			if(checkSameList(tmp, y))
				return true;
		}
		return false;
	}

	public static Overflow deserializeOverflow(String BucketName) {
		FileInputStream fileIn;
		String path = "src/main/resources/data/" + BucketName + ".ser";
		try {
			fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			System.out.println("Overflow Bucket " + BucketName + " is successfully deserialized in " + path);
			return (Overflow) in.readObject();

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}

		return null;
	}

	public static Bucket deserializeBucket(String BucketName) {
		FileInputStream fileIn;
		String path = "src/main/resources/data/" + BucketName + ".ser";
		try {
			fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			System.out.println("Bucket " + BucketName + " is successfully deserialized in " + path);
			return (Bucket) in.readObject();

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}

		return null;
	}
	public static void serialize(Grid grid,String tableName){
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
	public static void serialize(Table myTable,String tableName){
		FileOutputStream fileOut;
		ObjectOutputStream out;
		String path = "src/main/resources/data/" + tableName + ".ser";
		try {
			fileOut = new FileOutputStream( path);
			out = new ObjectOutputStream(fileOut);
			out.writeObject(myTable);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	public static Table deserialize(String tableName) {
		FileInputStream fileIn;
		String path = "src/main/resources/data/" + tableName + ".ser";
		try {
			fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			System.out.println("Table " + tableName + " is successfully deserialized in " + path);
			return (Table) in.readObject();

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}

		return null;
	}

	public static void serializeBucket(Bucket bucket, String BucketName) {
		FileOutputStream fileOut;
		ObjectOutputStream out;
		String path = "src/main/resources/data/" + BucketName + ".ser";
		try {
			fileOut = new FileOutputStream(path);
			out = new ObjectOutputStream(fileOut);
			out.writeObject(bucket);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	public static void serializeOverflow(Overflow overflow,String BucketName){
		FileOutputStream fileOut;
		ObjectOutputStream out;
		String path = "src/main/resources/data/" + BucketName + ".ser";
		try {
			fileOut = new FileOutputStream( path);
			out = new ObjectOutputStream(fileOut);
			out.writeObject(overflow);
			out.close();
			fileOut.close();
			System.out.println("Overflow bucket data is saved");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	public static HashSet<String> getPagesFromBucket(HashSet<String> hs){
		HashSet <String> allPages = new HashSet<String>();
		for(String bucket : hs){
			Bucket b = deserializeBucket(bucket);
			Vector<Entry> entries = b.entries;
			for(Entry e : entries){
				for(String s : e.pageAddress){
					allPages.add(s);
				}
			}
			if(b.overflow != null){
				for(String overflow : b.overflow){
					Overflow o = deserializeOverflow(overflow);
					for(Entry e : o.entries){
						for(String s : e.pageAddress){
							allPages.add(s);
						}
					}
				}
			}
		}


		return allPages;
	}

	public static double getValFromString(String val) {
		try {
			return getVal(Integer.parseInt(val));
		} catch (Exception e) {
			try {
				return getVal(Double.parseDouble(val));
			} catch (Exception ee) {
				try {
					String[] arr = val.split("-");
					Date min1 = new Date(Integer.parseInt(arr[0])- 1900, (Integer.parseInt(arr[1]) - 1), Integer.parseInt(arr[2]));
					return getVal(min1);

				} catch (Exception eee) {
					return getVal(val);
				}
			}

		}

	}

	public static double getVal(Object val) {

		if (val instanceof Integer){
			return (int) val;
		}
		else if (val instanceof Double){
			return (double) val;
		}
		else if (val instanceof String ){
			byte[] MinByte = ((String) val).getBytes();
			int min = 0;
			for(int i = 0 ; i < MinByte.length ; i++){
				min += MinByte[i];
			}
			return min;
		}
		else if (val instanceof Date) {
			long diffInMillies = ((Date) val).getTime();
			long diff = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
			return diff;

		}
		return 0;

	}
	
	public static void main(String[] args) throws IOException, DBAppException, ParseException {
		String [] arr = {"abc","aeq","asd"};
		Hashtable<String [] , String> ht= new Hashtable<>();
		ht.put(arr,"Grid1");
		System.out.println(ht.get(arr));
		Date d1 = new Date("1/1/1901");

		System.out.println(d1.getTime());


//		String s=getclusterkey("Student");
//		System.out.print(s);
//		Integer x=(Integer)0;
//		Integer y=(Integer)7;
//
//		Queue<String> p = new LinkedList<String>();
//
//		p.add("1");
////		p.add("1.1");
////		p.add("1.11");
////		p.add("2");
//
//		String a = "A";
//		String z = "ZZ";
//
//		byte[] min = a.getBytes("US-ASCII");
//		byte[] max = z.getBytes("US-ASCII");
//
//		int min1 =0;
//		int max1 = 0;
//		for(int i = 0 ; i < min.length ; i++){
//			min1 += min[i];
//		}
//		for(int i = 0 ; i < max.length ; i++){
//			max1 += max[i];
//		}
//
//
//
//
//
//
//		int[] sizes = new int[2];
//		for (int i = 0; i < sizes.length; i++)
//			sizes[i] = 2;
//		System.out.println("Creating array with dimensions / sizes: " + Arrays.toString(sizes).replaceAll(", ", "]["));
//		Object multiDimArray = Array.newInstance(Object.class, sizes);
//		System.out.println(Arrays.deepToString((Object[]) multiDimArray));
//
//
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//
//		Date date1 = new Date(2011 - 1900, 4 - 1, 1);
//		Date date2 = new Date(2012 - 1900, 4 - 1, 1);
//		System.out.println(getRange(date1,date2));
//
//		String [] arr = {"dob","gpa"};
//		rewriteMetaData("students" , arr);
	}




}
