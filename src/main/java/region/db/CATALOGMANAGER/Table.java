package region.db.CATALOGMANAGER;

import region.db.INDEXMANAGER.Index;
import region.db.CATALOGMANAGER.Attribute;

import java.util.Vector;

public class Table {

    public String tableName;
    public String primaryKey;
    public Vector<Attribute> attributeVector;
    public Vector<Index> indexVector;
    public int indexNum;
    public int attributeNum;
    //public TableRow tableRow;
    public int rowNum;
    public int rowLength;

    public Table(String tableName, String primaryKey, Vector<Attribute> attributeVector) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.indexVector = new Vector<>();
        this.indexNum = 0;
        this.attributeVector = attributeVector;
        this.attributeNum = attributeVector.size();
        this.rowNum = 0;
        for (int i = 0; i < attributeVector.size(); i++) {
            if (attributeVector.get(i).attributeName.equals(primaryKey))
                attributeVector.get(i).isUnique = true;
            this.rowLength += attributeVector.get(i).type.get_length();
        }
    }

    public Table(String tableName, String primaryKey, Vector<Attribute> attributeVector, Vector<Index> indexVector, int rowNum) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.attributeVector = attributeVector;
        this.indexVector = indexVector;
        this.indexNum = indexVector.size();
        this.attributeVector = attributeVector;
        this.attributeNum = attributeVector.size();
        this.rowNum = rowNum;
        for (int i = 0; i < attributeVector.size(); i++) {
            this.rowLength += attributeVector.get(i).type.get_length();
        }
    }
}