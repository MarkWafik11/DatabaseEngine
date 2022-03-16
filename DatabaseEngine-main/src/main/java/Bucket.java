import java.io.*;
import java.util.*;

public class Bucket implements Serializable {
    private static final long serialVersionUID = 2L;
    int size;
    String bucketName;
    Vector<Entry> entries;
    Vector<String> overflow;
    int overflowCounter;
    Hashtable<String , ArrayList<Object>> MinMaxSize;
    private Properties dbProps;


    public Bucket(String bucketName,String tableName){
        this.bucketName = bucketName;
        MinMaxSize = new Hashtable<>();
        overflowCounter = 0;
        overflow = new Vector<String>();
        entries = new Vector<>();
        try {
            dbProps = new java.util.Properties();
            FileInputStream fis = new FileInputStream("src/main/resources/DBApp.config");
            dbProps.load(fis);
        }
        catch (Exception e){
            System.out.println("DBApp.config is not found");
        }
        size = Integer.parseInt(dbProps.getProperty("MaximumKeysCountinIndexBucket"));
    }


    public Vector<String> linearSearchforEntries(Vector<Object> values){
        for(int i=0;i < this.entries.size();i++){
            Entry ofMid = this.entries.get(i);
            if(ofMid.indexCols.equals(values))
                return ofMid.pageAddress;
        }
        return null;
    }

    public Vector<String> getpagesofEntry(Vector<Object> values,String tablename,Grid g){
        Enumeration<String> enumeration = this.MinMaxSize.keys();
        int j = Helpers.getIndexingColumn(g.colNames,tablename);

        String bucketName = enumeration.nextElement();
        ArrayList<Object> minmaxsize = this.MinMaxSize.get(bucketName);
        if(Helpers.RangeWithinthepage(values.get(j),minmaxsize.get(0),minmaxsize.get(1))){
            Bucket b = Helpers.deserializeBucket(bucketName);
            return b.linearSearchforEntries(values);
        }
        else{
            while(enumeration.hasMoreElements()){
                String overflowName = enumeration.nextElement();
                minmaxsize = this.MinMaxSize.get(bucketName);
                if(Helpers.RangeWithinthepage(values.get(j),minmaxsize.get(0),minmaxsize.get(1))){
                    Overflow v = Helpers.deserializeOverflow(overflowName);
                    return v.linearSearchforEntries(values);
                }
            }
        }
        return null;
    }


    public void insertIntoBuckets(Vector<Object> indexCols, String pageName, int clusterKeyIndex) {
        if(this.entries.size() < size){ //there is at least one space for the entry
            Entry e = new Entry(indexCols,pageName,clusterKeyIndex);
            boolean flag = false;
            for(int i = 0 ; i < entries.size() ; i++){
                Entry e1 = this.entries.get(i);
                if(e1.indexCols.equals(indexCols)){
                    this.entries.get(i).pageAddress.add(pageName);
                    flag = true;
                    break;
                }
            }
            if(!flag)
                this.entries.add(e);
            Collections.sort(entries);
        }
        else{
            Overflow overflow1;
            Object x = indexCols.get(clusterKeyIndex);
            if(overflow.size() == 0){
                overflow1 = new Overflow(bucketName+"Overflow"+overflowCounter);
                overflow.add(bucketName+"Overflow"+overflowCounter);
                overflowCounter++;
                Entry lastEntry = entries.get(entries.size()-1);
                if(Helpers.XlessthanY(indexCols.get(clusterKeyIndex),lastEntry.sortingCol)){
                    int j=0;
                    for(int i = entries.size() / 2 ; i < entries.size() ; i++){
                        overflow1.entries.add(entries.get(i));
                        j++;
                    }
                    while(j != 0){
                        entries.remove(entries.lastElement());
                        j--;
                    }
                    java.util.Collections.sort(overflow1.entries);
                    Entry e = new Entry(indexCols,pageName,clusterKeyIndex);
                    if(Helpers.XlessthanY(indexCols.get(clusterKeyIndex),overflow1.entries.get(0))){
                        entries.add(e);
                        Collections.sort(entries);
                    }
                    else{
                        overflow1.entries.add(e);
                        java.util.Collections.sort(overflow1.entries);
                    }
                }
                else{
                    Entry e = new Entry(indexCols,pageName,clusterKeyIndex);
                    overflow1.entries.add(e);
                    Collections.sort(overflow1.entries);
                }
                Helpers.serializeOverflow(overflow1,bucketName+"Overflow"+overflowCounter);
            }
            else{
                int low = 0;
                int high = overflow.size()-1;
                int mid = (low + high) / 2;
                while(low <= high) {
                    mid = (high + low) / 2;
                    if(mid == overflow.size()){
                        mid--;
                        break;
                    }
                    String ofMid = overflow.get(mid);
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
                String overflowName = overflow.get(mid); // getting page name and deserialize it
                overflow1 = Helpers.deserializeOverflow(overflowName);
                if(overflow1.entries.size() < overflow1.size){
                    Entry e = new Entry(indexCols,pageName,clusterKeyIndex);
                    overflow1.entries.add(e);
                    Collections.sort(overflow1.entries);
                }
                else{
                    Overflow overflow2 = new Overflow(bucketName+"Overflow"+overflowCounter);
                    overflow.add(mid+1,bucketName+"Overflow"+overflowCounter);
                    overflowCounter++;
                    int j=0;
                    for(int i = overflow1.entries.size() / 2 ; i < overflow1.entries.size() ; i++){
                        overflow2.entries.add(overflow1.entries.get(i));
                        j++;
                    }
                    while(j != 0){
                        overflow1.entries.remove(overflow1.entries.lastElement());
                        j--;
                    }
                    java.util.Collections.sort(overflow2.entries);
                    Entry e = new Entry(indexCols,pageName,clusterKeyIndex);
                    if(Helpers.XlessthanY(indexCols.get(clusterKeyIndex),overflow2.entries.get(0))){ //insert in overflow1
                        boolean flag = false;
                        for(int i = 0 ; i < overflow1.entries.size() ; i++){
                            Entry e1 = overflow1.entries.get(i);
                            if(e1.indexCols.equals(indexCols)){
                                overflow1.entries.get(i).pageAddress.add(pageName);
                                flag = true;
                                break;
                            }
                        }
                        if(!flag)
                            overflow1.entries.add(e);
                        Collections.sort(overflow1.entries);
                    }
                    else{
                        boolean flag = false;
                        for(int i = 0 ; i < overflow2.entries.size() ; i++){
                            Entry e1 = overflow2.entries.get(i);
                            if(e1.indexCols.equals(indexCols)){
                                overflow2.entries.get(i).pageAddress.add(pageName);
                                flag = true;
                                break;
                            }
                        }
                        if(!flag)
                            overflow2.entries.add(e);
                        Collections.sort(overflow2.entries);
                    }
                    Helpers.serializeOverflow(overflow1,overflow1.OverflowName);
                    Helpers.serializeOverflow(overflow2,overflow2.OverflowName);

                    ArrayList<Object> newArray = new ArrayList<Object>(3);
                    newArray.add(overflow2.entries.firstElement().sortingCol); //Min
                    newArray.add(overflow2.entries.lastElement().sortingCol); //Max
                    newArray.add(overflow2.entries.size()); //Size
                    MinMaxSize.put(bucketName,newArray);
                }
            }

            ArrayList<Object> newArray;
            newArray = new ArrayList<Object>(3);
            newArray.add(overflow1.entries.firstElement().sortingCol); //Min
            newArray.add(overflow1.entries.lastElement().sortingCol); //Max
            newArray.add(overflow1.entries.size()); //Size
            MinMaxSize.put(overflow1.OverflowName,newArray);
        }
        ArrayList<Object> newArray = new ArrayList<Object>(3);
        newArray.add(this.entries.firstElement().sortingCol); //Min
        newArray.add(this.entries.lastElement().sortingCol); //Max
        newArray.add(this.entries.size()); //Size
        MinMaxSize.put(bucketName,newArray);

    }

    public void DeleteFromBuckets(Vector<Object> values, int j) {
       if(entries.contains(values)){
           entries.remove(values);
       }
       else{
           for(String o : overflow){
               Overflow o1 = Helpers.deserializeOverflow(o);
               if(o1.entries.contains(values)){
                   o1.entries.remove(values);
                   if(o1.entries.size()==0){
                       overflow.remove(o);
                       MinMaxSize.remove(o);
                   }
               }
               break;
           }
       }
    }



    public static void main(String[] args) {
        Bucket b = new Bucket("B1","Trial");
        Vector<Object> entry1 = new Vector<>();
        entry1.add(1);
        entry1.add("George");


        Vector<Object> entry2 = new Vector<>();
        entry2.add(2);
        entry2.add("George");

        Vector<Object> entry3 = new Vector<>();
        entry3.add(3);
        entry3.add("George");

        Vector<Object> entry4 = new Vector<>();
        entry4.add(4);
        entry4.add("George");

        Vector<Object> entry5 = new Vector<>();
        entry5.add(5);
        entry5.add("George");

        Vector<Object> entry6 = new Vector<>();
        entry6.add(6);
        entry6.add("George");

        Vector<Object> entry7 = new Vector<>();
        entry7.add(6);
        entry7.add("George");

        Vector<Object> entry8 = new Vector<>();
        entry8.add(1);
        entry8.add("George");

        b.insertIntoBuckets(entry1,"P1" , 0);
        b.insertIntoBuckets(entry2,"P3" , 0);
        b.insertIntoBuckets(entry3,"P5" , 0);
        b.insertIntoBuckets(entry4,"P4" , 0);
        b.insertIntoBuckets(entry5,"P1" , 0);
        b.insertIntoBuckets(entry6,"P2" , 0);
        b.insertIntoBuckets(entry7,"P3" , 0);
        b.insertIntoBuckets(entry6,"P2" , 0);
        b.insertIntoBuckets(entry8,"P6" , 0);




        for(Entry e : b.entries){
            System.out.println(e.indexCols+" "+e.pageAddress);
        }

    }


}
