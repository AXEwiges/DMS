package region.db.RECORDMANAGER;

import region.db.CATALOGMANAGER.CatalogManager;

public class Condition {
    private String name;  //attribute name in condition
    private String value; //attribute value in condition
    private String operator; //condition operator

    public Condition() {
        //do nothing
    }

    public Condition(String name, String operator, String value) {
        this.name = name; //initialize name, operator and value
        this.operator = operator;
        this.value = value;
    }

    //if the data in table satisfy the condition, return true, else return false 
    public boolean satisfy(String tableName, TableRow data) {
        int index = CatalogManager.get_attribute_index(tableName, this.name); //get attribute index
        String type = CatalogManager.get_type(tableName, index); //get type

        if (type.equals("CHAR")) { //char type
            String cmpObject = data.get_attribute_value(index);
            String cmpValue = this.value;

            if (this.operator.equals("=")) {
                return cmpObject.compareTo(cmpValue) == 0;
            } else if (this.operator.equals("<>")) {
                return cmpObject.compareTo(cmpValue) != 0;
            } else if (this.operator.equals(">")) {
                return cmpObject.compareTo(cmpValue) > 0;
            } else if (this.operator.equals("<")) {
                return cmpObject.compareTo(cmpValue) < 0;
            } else if (this.operator.equals(">=")) {
                return cmpObject.compareTo(cmpValue) >= 0;
            } else if (this.operator.equals("<=")) {
                return cmpObject.compareTo(cmpValue) <= 0;
            } else { //undefined operator
                return false;
            }
        } else if (type.equals("INT")) { //integer type
            int cmpObject = Integer.parseInt(data.get_attribute_value(index));
            int cmpValue = Integer.parseInt(this.value);
            switch (this.operator) {
                case "=":
                    return cmpObject == cmpValue;
                case "<>":
                    return cmpObject != cmpValue;
                case ">":
                    return cmpObject > cmpValue;
                case "<":
                    return cmpObject < cmpValue;
                case ">=":
                    return cmpObject >= cmpValue;
                case "<=":
                    return cmpObject <= cmpValue;
                default:
                    return false;
            }
        } else if (type.equals("FLOAT")) { //float type
            float cmpObject = Float.parseFloat(data.get_attribute_value(index));
            float cmpValue = Float.parseFloat(this.value);
            if (this.operator.equals("=")) {
                return cmpObject == cmpValue;
            } else if (this.operator.equals("<>")) {
                return cmpObject != cmpValue;
            } else if (this.operator.equals(">")) {
                return cmpObject > cmpValue;
            } else if (this.operator.equals("<")) {
                return cmpObject < cmpValue;
            } else if (this.operator.equals(">=")) {
                return cmpObject >= cmpValue;
            } else if (this.operator.equals("<=")) {
                return cmpObject <= cmpValue;
            } else { //undefined operator
                return false;
            }
        } else { //undefined type
            return false;
        }
    }


    public String get_name() {
        return this.name;
    }

    public String get_value() {
        return this.value;
    }

    public String get_operator() {
        return this.operator;
    }

    public void set_name(String name) {
        this.name = name;
    }

    public void set_value(String value) {
        this.value = value;
    }

    public void set_operator(String operator) {
        this.operator = operator;
    }
}
