import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;


public class Record implements Comparable, Serializable {
	
	Vector<Object> values;
	String tablename;
	Object clusterkey;
	
	public Record(String tableName,Object clusterkey)  {
		this.tablename = tableName;
		values = new Vector<Object>();
		this.clusterkey=clusterkey;
	}
	
	public void insert(Object o){
		values.add(o);
	}
	
	

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Record tmp = (Record) o;  
		
		if (clusterkey instanceof Integer)
			 return ((Integer)this.clusterkey).compareTo((Integer)tmp.clusterkey);
		if (clusterkey instanceof String)
			 return ((String)this.clusterkey).compareTo((String)tmp.clusterkey);
		if (clusterkey instanceof Double)
			 return ((Double)this.clusterkey).compareTo((Double)tmp.clusterkey);
		
		if (clusterkey instanceof Boolean)
			 return ((Boolean)this.clusterkey).compareTo((Boolean)tmp.clusterkey);
       
		if (clusterkey instanceof java.util.Date)
			 return ((java.util.Date)this.clusterkey).compareTo((java.util.Date)tmp.clusterkey);
		
		return 1;
	}

	public static void main(String[] args) {
		Vector v = new Vector();
		Hashtable ht = new Hashtable<String,Object>();
		v.add(null);
		v.add(null);
		v.add(ht);
		System.out.println(v);
	}
	
	
}
