public class SQLTerm {
    String _strTableName = "";
    String _strColumnName= "";
    String _strOperator = "";
    Object _objValue = null;

    public SQLTerm(String tableName , String columnName , String opreator, Object val){
        this._strTableName = tableName;
        this._strColumnName = columnName;
        this._strOperator = opreator;
        this._objValue = val ;

    }
    public SQLTerm(){

    }

    public String get_strTableName() {
        return _strTableName;
    }

    public String get_strColumnName() {
        return _strColumnName;
    }

    public String get_strOperator() {
        return _strOperator;
    }

    public Object get_objValue() {
        return _objValue;
    }


}
