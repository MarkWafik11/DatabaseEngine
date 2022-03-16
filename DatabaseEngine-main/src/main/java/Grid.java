import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Grid implements Serializable{
    String name;
    double[] range; //[20,36,50]
    String[] colNames; //[age,col2,col3]
    String[] index; //[bucket1,bucket2,bucket3] //slots of the grid
    int BucketCounter;

    public Grid(int indexDimension, String name, String[] columnNames, String Gridname) {
        this.name = Gridname;
        colNames = columnNames;
        BucketCounter = 1;
        index = new String[(int) Math.pow(10, indexDimension)];
        range = new double[indexDimension];
        Hashtable<String, Vector<String>> minMaxChecker = Helpers.getMetaData3(name);
        for (int i = 0; i < columnNames.length; i++) {
            Double min = Helpers.getValFromString(minMaxChecker.get(columnNames[i]).get(0));
            Double max = Helpers.getValFromString(minMaxChecker.get(columnNames[i]).get(1));
            range[i] = Math.abs((max - min) / 10.0);
        }
    }

    public int getIndexInBucket(Vector<Object> values, String TableName){
        int counter = 0;
        int factor = 1;
        for (int i = 0; i < colNames.length; i++) {
            String name = colNames[i];
            System.out.println("This is the value " + values.get(i) );
            double value = Helpers.getVal(values.get(i));
            if(value <= Helpers.getMaximumValue(name,TableName) && value >= Helpers.getMinimumValue(name,TableName)){
            System.out.println("This is the value " + value +" in the " +i+"th iteration");
            System.out.println("This is the maximum of Col: " + colNames[i]+ " "+ Helpers.getMaximumValue(name,TableName));
            System.out.println("This is the minimum of Col: " + colNames[i]+ " "+Helpers.getMinimumValue(name,TableName));
            counter += factor * (Math.floor(Math.abs(value - Helpers.getMinimumValue(name,TableName)) / (range[i])));
            System.out.println("This is the factor of Col: " + colNames[i]+ " "+ + factor);
            System.out.println("This is the range of Col: " + colNames[i]+ " "+ + range[i]);
            System.out.println("This is the counter " + counter);

            if(Helpers.XequalY(Helpers.getMaximumValue(name,TableName),value)){
                counter -= factor;
            }
            factor *= 10;
        }}
        return counter;
    }

    public void insertIntoBucket(Vector<Object> values, String pageAddress, Table table) {
        int counter = getIndexInBucket(values,table.name);
        Bucket b;
        if (index[counter] == null) {
            String bucketName = table.name + "Bucket" + BucketCounter;
            BucketCounter++;
            index[counter] = bucketName;
            b = new Bucket(bucketName, table.name);
        } else {
            b = Helpers.deserializeBucket(index[counter]);
        }

        int j = Helpers.getIndexingColumn(colNames,table.name);

        b.insertIntoBuckets(values, pageAddress, j);
        Helpers.serializeBucket(b, b.bucketName);

    }

    public void deleteFromBucket(Hashtable colNameValues, Table table) {
        Vector<Object> values = new Vector<Object>();
        for (String col : colNames) {
            values.add(colNameValues.get(col));
        }
        int counter = getIndexInBucket(values,table.name);
        Bucket b;
        if (index[counter] == null) {
            String bucketName = table.name + "Bucket" + BucketCounter;
            BucketCounter++;
            index[counter] = bucketName;
            b = new Bucket(bucketName, table.name);
        } else {
            b = Helpers.deserializeBucket(index[counter]);
        }
        int j = 0;
        for (int i = 0; i < colNames.length; i++) {
            if (colNames[i].equals(Helpers.getclusterkey(table.name))) {
                j = i;
                break;
            }
        }
        b.DeleteFromBuckets(values, j);
       Helpers.serializeBucket(b, b.bucketName);

    }

    public void populateGrid(Table t) throws DBAppException {
        int counter = 0;
        int factor = 1;
        Vector<Object> values = new Vector<>();
        for (String page : t.positions) {
            Page p = t.deserializePage(page);
            for (Record r : p.records) {  //40,"George",21
                for (int i = 0; i < colNames.length; i++) {
                    String name = colNames[i];
                    double value = Helpers.getVal(r.values.get(p.columns.indexOf(name)));
                    values.add(value);
                    System.out.println("This is the value " + value +" in the " +i+"th iteration");
                    System.out.println("This is the maximum of Col: " + colNames[i]+ " "+ Helpers.getMaximumValue(name,t.name));
                    System.out.println("This is the minimum of Col: " + colNames[i]+ " "+Helpers.getMinimumValue(name,t.name));
                    counter += factor * (Math.floor(Math.abs(value - Helpers.getMinimumValue(name,t.name)) / (range[i])));
                    System.out.println("This is the factor of Col: " + colNames[i]+ " "+ + factor);
                    System.out.println("This is the range of Col: " + colNames[i]+ " "+ + range[i]);
                    System.out.println("This is the counter " + counter);

                    if(Helpers.XequalY(Helpers.getMaximumValue(name,t.name),value)){
                        counter -= factor;
                    }

                    factor *= 10;

                }

                Bucket b;

                if (index[counter] == null) {
                    String bucketName = t.name + "Bucket" + BucketCounter;
                    BucketCounter++;
                    index[counter] = bucketName;
                    b = new Bucket(bucketName, t.name);
                } else {
                    b = Helpers.deserializeBucket(index[counter]);
                }
                int j = 0;
                for (int i = 0; i < colNames.length; i++) {
                    if (colNames[i].equals(Helpers.getclusterkey(t.name))) {
                        j = i;
                        break;
                    }
                }
                b.insertIntoBuckets(values, p.PageName, j);
                values = new Vector<>();
                counter = 0;
                factor = 1;
                Helpers.serializeBucket(b, b.bucketName);
            }
            t.tableGrids.put(colNames,name);
        }
    }

    //rule for finding the bucket counter = i + j * 10

        //rule for finding the index = floor((Value - min) / range)
        // 21-30 , 31-40 , 41-50

        // 1-3 , 4-6 , 7-9 , 10-12 , 13-15
        //  0     1     2     3        4
}




