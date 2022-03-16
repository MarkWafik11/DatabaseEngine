import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Page implements Serializable, Comparable{
	private static final long serialVersionUID = 2L;
	String PageName;
	int maxSize;
	String TableName;
	Vector<Record> records;
	Vector<String> columns;
	Object Min;
	private Properties dbProps;



	public Page(String PageName, String TableName) {
		try {
			dbProps = new java.util.Properties();
			FileInputStream fis = new FileInputStream("src/main/resources/DBApp.config");
			dbProps.load(fis);
		}
		catch (Exception e){
			System.out.println("DBApp.config is not found");
		}
		maxSize = Integer.parseInt(dbProps.getProperty("MaximumRowsCountinPage"));
		this.PageName = PageName;
		this.TableName = TableName;
		records = new Vector<Record>();
		columns =Helpers.getMetaData2(TableName);
	}

	public boolean isEmpty(){
		return records.size() == 0;
	}
	
	public Record get(int idx){
		if(idx >= 0 && idx < this.size())
			return records.get(idx);
		throw new IndexOutOfBoundsException(""+idx);
	}
	
	public int size(){
		return records.size();
	}

	public void insertIntoPage (Hashtable ht , String TableName) {

		String s =Helpers.getclusterkey(TableName);
		Object clusterkey=null;
		for(int i=0;i<columns.size();i++){
			if(columns.get(i).equals(s)){
				 clusterkey=ht.get(s);
			}						
		}
		Record newrec =new Record(TableName,clusterkey);
		for(int i=0;i<columns.size();i++){
			newrec.insert(ht.get(columns.get(i)));;						
		}
		this.records.add(newrec);

		java.util.Collections.sort(records);
	}

	public Object getmaxValue (){
		return Helpers.getclusterkeyValue2(this.records.lastElement() ,TableName);
	}

	public Object getminValue (){
		return Helpers.getclusterkeyValue2(this.records.firstElement() ,TableName) ;
	}
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Page tmp = (Page) o;

		if (Min instanceof Integer)
			return ((Integer)this.Min).compareTo((Integer)tmp.Min);
		if (Min instanceof String)
			return ((String)this.Min).compareTo((String)tmp.Min);
		if (Min instanceof Double)
			return ((Double)this.Min).compareTo((Double)tmp.Min);

		if (Min instanceof Boolean)
			return ((Boolean)this.Min).compareTo((Boolean)tmp.Min);

		if (Min instanceof Date)
			return ((Date)this.Min).compareTo((Date)tmp.Min);

		return 1;
	}
	
	public static void main(String[] args) throws IOException, DBAppException {
		
		Table t=new Table("Student","id");
		Hashtable colNameMax = new Hashtable( );
		colNameMax.put("id", 1);
		colNameMax.put("name", "ZZZZZZZZZZZ");
		colNameMax.put("gpa","5");
		
		Hashtable colNameMax2 = new Hashtable( );
		colNameMax2.put("id", 5);
		colNameMax2.put("name", "ZZZZZZZZZZZ");
		colNameMax2.put("gpa","4");
		
		Hashtable colNameMax3 = new Hashtable( );
		colNameMax3.put("id", 7);
		colNameMax3.put("name", "ZZZZZZZZZZZ");
		colNameMax3.put("gpa","5");
		
		Hashtable colNameMax4 = new Hashtable( );
		colNameMax4.put("id", 8);
		colNameMax4.put("name", "ZZZZZZZZZZZ");
		colNameMax4.put("gpa","4");
		
		Hashtable colNameMax5 = new Hashtable( );
		colNameMax5.put("id", 3);
		colNameMax5.put("name", "ZZZZZZZZZZZ");
		colNameMax5.put("gpa","4");
		Hashtable colNameMax6 = new Hashtable( );
		colNameMax6.put("id", 2);
		colNameMax6.put("name", "ZZZZZZZZZZZ");
		colNameMax6.put("gpa","4");
		Hashtable colNameMax7 = new Hashtable( );
		colNameMax7.put("id",10);
		colNameMax7.put("name", "ZZZZZZZZZZZ");
		colNameMax7.put("gpa","4");
		Hashtable colNameMax8 = new Hashtable( );
		colNameMax8.put("id", 4);
		colNameMax8.put("name", "ZZZZZZZZZZZ");

		Hashtable colNameMax9 = new Hashtable( );
		colNameMax9.put("id", 0);
		colNameMax9.put("name", "ZZZZZZZZZZZ");
		colNameMax9.put("gpa","4");

		Hashtable colNameMax10 = new Hashtable( );
		colNameMax10.put("id", -1);
		colNameMax10.put("name", "Kerz");
		colNameMax10.put("gpa","10");


		t.insertIntoPages (colNameMax ,"Student"); //1
		t.insertIntoPages (colNameMax2 ,"Student"); //5
		t.insertIntoPages (colNameMax3 ,"Student"); //7
		t.insertIntoPages (colNameMax4 ,"Student"); //8
		t.insertIntoPages (colNameMax5 ,"Student"); //3
		t.insertIntoPages (colNameMax6 ,"Student"); //2
		t.insertIntoPages (colNameMax7 ,"Student"); //10
		t.insertIntoPages (colNameMax8 ,"Student"); //4
		t.insertIntoPages (colNameMax9 ,"Student");
		t.insertIntoPages (colNameMax10 ,"Student");
		

		

		
		for(int i = 0 ; i < t.positions.size(); i++) {
			Page p = t.deserializePage(t.positions.get(i));
			System.out.println(p.PageName);
			for(int j = 0 ; j < p.records.size() ; j++) {
				Record r = p.records.get(j);
				System.out.println(r.values.get(2));

			}

		}

		System.out.println("--------------------------------------");

		String table = "Student";
		Hashtable<String, Object> row = new Hashtable();
		row.put("name", "ZZZZZZZZZZZ");
		row.put("gpa", "5");
		t.deleteFromPages(row,"Student");
		for(int i = 0 ; i < t.positions.size(); i++) {
			Page p = t.deserializePage(t.positions.get(i));
			System.out.println(p.PageName);
			for(int j = 0 ; j < p.records.size() ; j++) {
				Record r = p.records.get(j);
				System.out.println(r.values.get(2));

			}

		}


		//t.deleteFromPages(colNameMax10, "Student");
		//t.UpdatePages(colNameMax10, "Student", "id");
//		for(int i = 0 ; i < t.pages.size(); i++) {
//			System.out.println(t.pages.get(i).records);
//			for(int j = 0 ; j < t.pages.get(i).size() ; j++) {
//				System.out.println(t.pages.get(i).records.get(j).values);
//				Record r =t.pages.get(i).records.get(j);
//				System.out.println(r.values.get(2));
//			}
			
		//}
	}



}
