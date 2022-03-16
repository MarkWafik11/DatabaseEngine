import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Overflow implements Serializable  {
    private static final long serialVersionUID = 2L;
    int size;
    String OverflowName;
    Vector<Entry> entries;
    private Properties dbProps;


    public Overflow(String OverflowName){
        this.OverflowName = OverflowName;
        entries = new Vector<>();
        try {
            dbProps = new java.util.Properties();
            FileInputStream fis = new FileInputStream("src/main/resources/DBApp.config");
            dbProps.load(fis);
        }
        catch (Exception e){
            System.out.println("DBApp.config is not found");
        }
        size = Integer.parseInt(dbProps.getProperty("MaximumKeysCountinIndexOverflow"));
    }

    public Vector<String> linearSearchforEntries(Vector<Object> values){
        for(int i=0;i < this.entries.size();i++){
            Entry ofMid = this.entries.get(i);
            if(ofMid.indexCols.equals(values))
                return ofMid.pageAddress;
        }
        return null;
    }

}
