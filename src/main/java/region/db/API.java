package region.db;

import region.db.BUFFERMANAGER.BufferManager;
import region.db.CATALOGMANAGER.Address;
import region.db.CATALOGMANAGER.CatalogManager;
import region.db.CATALOGMANAGER.Table;
import region.db.INDEXMANAGER.Index;
import region.db.INDEXMANAGER.IndexManager;
import region.db.RECORDMANAGER.Condition;
import region.db.RECORDMANAGER.RecordManager;
import region.db.RECORDMANAGER.TableRow;

import java.io.IOException;
import java.util.Vector;

public class API {

    public void initial() throws Exception {
	    try {
		    BufferManager.initial_buffer();  //init Buffer Manager
		    CatalogManager.initial_catalog();  //init Catalog Manager
		    IndexManager.initial_index(); //init Index Manager
	    } catch (Exception e) {
		    throw new QException(1, 500, "Failed to initialize API!");
	    }
    }

    public API() throws Exception {
        initial();
    }

    public void store() throws Exception {
	    CatalogManager.store_catalog();
	    RecordManager.store_record();
    }

    public boolean create_table(String tabName, Table tab) throws Exception {
        try {
            if (RecordManager.create_table(tabName) && CatalogManager.create_table(tab)) {
                String indexName = tabName + "_index";  //refactor index name
                Index index = new Index(indexName, tabName, CatalogManager.get_primary_key(tabName));
                IndexManager.create_index(index);  //create index on Index Manager
                CatalogManager.create_index(index); //create index on Catalog Manager
                return true;
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw new QException(1, 501, "Table " + tabName + " already exist!");
        } catch (IOException e) {
            throw new QException(1, 502, "Failed to create an index on table " + tabName);
        }
        throw new QException(1, 503, "Failed to create table " + tabName);
    }

    public boolean drop_table(String tabName) throws Exception {
        try {
            for (int i = 0; i < CatalogManager.get_attribute_num(tabName); i++) {
                String attrName = CatalogManager.get_attribute_name(tabName, i);
                String indexName = CatalogManager.get_index_name(tabName, attrName);  //find index if exists
                if (indexName != null) {
                    IndexManager.drop_index(CatalogManager.get_index(indexName)); //drop index at Index Manager
                }
            }
            if (CatalogManager.drop_table(tabName) && RecordManager.drop_table(tabName)) return true;
        } catch (NullPointerException e) {
            throw new QException(1, 504, "Table " + tabName + " does not exist!");
        }
        throw new QException(1, 505, "Failed to drop table " + tabName);
    }

    public boolean create_index(Index index) throws Exception {
        if (IndexManager.create_index(index) && CatalogManager.create_index(index)) return true;
        throw new QException(1, 506, "Failed to create index " + index.attributeName + " on table " + index.tableName);
    }

    public boolean drop_index(String indexName) throws Exception {
        Index index = CatalogManager.get_index(indexName);
        if (IndexManager.drop_index(index) && CatalogManager.drop_index(indexName)) return true;
        throw new QException(1, 507, "Failed to drop index " + index.attributeName + " on table " + index.tableName);
    }

    public boolean insert_row(String tabName, TableRow row) throws Exception {
        try {
            Address recordAddr = RecordManager.insert(tabName, row);  //insert and get return address
            int attrNum = CatalogManager.get_attribute_num(tabName);  //get the number of attribute
            for (int i = 0; i < attrNum; i++) {
                String attrName = CatalogManager.get_attribute_name(tabName, i);
                String indexName = CatalogManager.get_index_name(tabName, attrName);  //find index if exists
                if (indexName != null) {  //index exists, then need to insert the key to BPTree
                    Index index = CatalogManager.get_index(indexName); //get index
                    String key = row.get_attribute_value(i);  //get value of the key
                    IndexManager.insert(index, key, recordAddr);  //insert to index manager
                    CatalogManager.update_index_table(indexName, index); //update index
                }
            }
            CatalogManager.add_row_num(tabName);  //update number of records in catalog        return true;
            return true;
        } catch (NullPointerException e){
	        throw new QException(1, 508, "Table " + tabName + " does not exist!");
        } catch (IllegalArgumentException e) {
        	throw new QException(1, 509, e.getMessage());
        } catch (Exception e) {
            throw new QException(1, 510, "Failed to insert a row on table " + tabName);
        }
    }

    public int delete_row(String tabName, Vector<Condition> conditions) throws Exception {
        Condition condition = find_index_condition(tabName, conditions);
        int numberOfRecords = 0;
        if (condition != null) {
            try {
                String indexName = CatalogManager.get_index_name(tabName, condition.get_name());
                Index idx = CatalogManager.get_index(indexName);
                Vector<Address> addresses = IndexManager.select(idx, condition);
                if (addresses != null) {
                    numberOfRecords = RecordManager.delete(addresses, conditions);
                }
            } catch (NullPointerException e) {
	            throw new QException(1, 511, "Table " + tabName + " does not exist!");
            } catch (IllegalArgumentException e) {
	            throw new QException(1, 512, e.getMessage());
            } catch (Exception e) {
                throw new QException(1, 513, "Failed to delete on table " + tabName);
            }
        } else {
            try {
            	numberOfRecords = RecordManager.delete(tabName, conditions);
            }  catch (NullPointerException e) {
	            throw new QException(1, 514, "Table " + tabName + " does not exist!");
            } catch (IllegalArgumentException e) {
	            throw new QException(1, 515, e.getMessage());
            }
        }
        CatalogManager.delete_row_num(tabName, numberOfRecords);
        return numberOfRecords;
    }

    public Vector<TableRow> select(String tabName, Vector<String> attriName, Vector<Condition> conditions) throws Exception {
	    Vector<TableRow> resultSet = new Vector<>();
	    Condition condition = find_index_condition(tabName, conditions);
	    if (condition != null) {
		    try {
			    String indexName = CatalogManager.get_index_name(tabName, condition.get_name());
			    Index idx = CatalogManager.get_index(indexName);
			    Vector<Address> addresses = IndexManager.select(idx, condition);
			    if (addresses != null) {
				    resultSet = RecordManager.select(addresses, conditions);
			    }
		    } catch (NullPointerException e) {
			    throw new QException(1, 516, "Table " + tabName + " does not exist!");
		    } catch (IllegalArgumentException e) {
			    throw new QException(1, 517, e.getMessage());
		    } catch (Exception e) {
			    throw new QException(1, 518, "Failed to select from table " + tabName);
		    }
	    } else {
		    try {
			    resultSet = RecordManager.select(tabName, conditions);
		    } catch (NullPointerException e) {
			    throw new QException(1, 519, "Table " + tabName + " does not exist!");
		    } catch (IllegalArgumentException e) {
			    throw new QException(1, 520, e.getMessage());
		    }
	    }

	    if (!attriName.isEmpty()) {
		    try {
			    return RecordManager.project(tabName, resultSet, attriName);
		    } catch (NullPointerException e) {
			    throw new QException(1, 521, "Table " + tabName + " does not exist!");
		    } catch (IllegalArgumentException e) {
			    throw new QException(1, 522, e.getMessage());
		    }
	    } else {
		    return resultSet;
	    }

    }

    private Condition find_index_condition(String tabName, Vector<Condition> conditions) throws Exception {
        Condition condition = null;
        for (int i = 0; i < conditions.size(); i++) {
            if (CatalogManager.get_index_name(tabName, conditions.get(i).get_name()) != null) {
                condition = conditions.get(i);
                conditions.remove(condition);
                break;
            }
        }
        return condition;
    }


}
