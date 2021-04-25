package bean;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

public class ColumnData implements Serializable {

    private List<String> columnName;

    private List<LinkedHashMap<String,String>> columnDataList;

    private List<List<String>> dataList;

    public List<String> getColumnName() {
        return columnName;
    }

    public void setColumnName(List<String> columnName) {
        this.columnName = columnName;
    }

    public List<LinkedHashMap<String, String>> getColumnDataList() {
        return columnDataList;
    }

    public void setColumnDataList(List<LinkedHashMap<String, String>> columnDataList) {
        this.columnDataList = columnDataList;
    }

    public List<List<String>> getDataList() {
        return dataList;
    }

    public void setDataList(List<List<String>> dataList) {
        this.dataList = dataList;
    }
}
