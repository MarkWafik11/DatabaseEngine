import java.io.Serializable;
import java.util.Vector;

public class Entry implements Comparable, Serializable {
    int clusterKeyIndex;
    Vector<Object> indexCols;
    Vector<String> pageAddress = new Vector<>();
    Object sortingCol;

    public Entry(Vector<Object> indexCols, String pageName,int clusterKeyIndex){
        this.indexCols = indexCols;
        this.clusterKeyIndex = clusterKeyIndex;
        this.sortingCol = indexCols.get(clusterKeyIndex);
        this.pageAddress.add(pageName);
    }

    @Override
    public int compareTo(Object o) {
        Entry tmp = (Entry) o;
        if (sortingCol instanceof Integer)
            return ((Integer) this.sortingCol).compareTo((Integer)tmp.sortingCol);
        if (sortingCol instanceof String)
            return ((String) this.sortingCol).compareTo((String)tmp.sortingCol);
        if (sortingCol instanceof Double)
            return ((Double) this.sortingCol).compareTo((Double) tmp.sortingCol);
        if (sortingCol instanceof Boolean)
            return ((Boolean)this.sortingCol).compareTo((Boolean)tmp.sortingCol);

        if (sortingCol instanceof java.util.Date)
            return ((java.util.Date)this.sortingCol).compareTo((java.util.Date)tmp.sortingCol);

        return 1;
    }




}
