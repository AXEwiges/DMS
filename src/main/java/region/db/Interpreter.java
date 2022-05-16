package region.db;

import region.db.CATALOGMANAGER.Attribute;
import region.db.CATALOGMANAGER.CatalogManager;
import region.db.CATALOGMANAGER.NumType;
import region.db.CATALOGMANAGER.Table;
import region.db.INDEXMANAGER.Index;
import region.db.INDEXMANAGER.IndexManager;
import region.db.RECORDMANAGER.Condition;
import region.db.RECORDMANAGER.RecordManager;
import region.db.RECORDMANAGER.TableRow;
import region.rpc.execResult;

import java.io.*;
import java.util.Arrays;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Interpreter {

    private static boolean nestLock = false; //not permit to use nesting sql file execution
    private static int execFile = 0;

    public API api;

    public Interpreter() throws Exception {
        api = new API();
    }
    public static void main(String[] args) {
//        try {
//            API.initial();
//            System.out.println("Welcome to minisql~");
//            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//            interpret(reader);
//        } catch (IOException e) {
//            System.out.println("101 Run time error : IO exception occurs");
//        } catch (Exception e) {
//            System.out.println("Default error: " + e.getMessage());
//        }

    }

    private void interpret(BufferedReader reader) throws IOException {
        String restState = ""; //rest statement after ';' in last line

        while (true) { //read for each statement
            int index;
            String line;
            StringBuilder statement = new StringBuilder();
            if (restState.contains(";")) { // resetLine contains whole statement
                index = restState.indexOf(";");
                statement.append(restState.substring(0, index));
                restState = restState.substring(index + 1);
            } else {
                statement.append(restState); //add rest line
                statement.append(" ");
                if (execFile == 0)
                    System.out.print("MiniSQL-->");
//                System.out.print("-->");
                while (true) {  //read whole statement until ';'
                    line = reader.readLine();
                    if (line == null) { //read the file tail
                        reader.close();
                        return;
                    } else if (line.contains(";")) { //last line
                        index = line.indexOf(";");
                        statement.append(line.substring(0, index));
                        restState = line.substring(index + 1); //set reset statement
                        break;
                    } else {
                        statement.append(line);
                        statement.append(" ");
                        if (execFile == 0)
                            System.out.print("MiniSQL-->");
//                        System.out.print("-->"); //next line
                    }
                }
            }

            //after get the whole statement
            String result = statement.toString().trim().replaceAll("\\s+", " ");
            String[] tokens = result.split(" ");

            try {
                if (tokens.length == 1 && tokens[0].equals(""))
                    throw new QException(0, 200, "No statement specified");
                switch (tokens[0]) { //match keyword
                    case "create":
                        if (tokens.length == 1)
                            throw new QException(0, 201, "Can't find create object");
                        switch (tokens[1]) {
                            case "table":
                                parse_create_table(result);
                                break;
                            case "index":
                                parse_create_index(result);
                                break;
                            default:
                                throw new QException(0, 202, "Can't identify " + tokens[1]);
                        }
                        break;
                    case "drop":
                        if (tokens.length == 1)
                            throw new QException(0, 203, "Can't find drop object");
                        switch (tokens[1]) {
                            case "table":
                                parse_drop_table(result);
                                break;
                            case "index":
                                parse_drop_index(result);
                                break;
                            default:
                                throw new QException(0, 204, "Can't identify " + tokens[1]);
                        }
                        break;
                    case "select":
                        parse_select(result);
                        break;
                    case "insert":
                        parse_insert(result);
                        break;
                    case "delete":
                        parse_delete(result);
                        break;
                    case "quit":
                        parse_quit(result, reader);
                        break;
                    case "execfile":
                        parse_sql_file(result);
                        break;
                    case "show":
                        parse_show(result);
                        break;
                    default:
                        throw new QException(0, 205, "Can't identify " + tokens[0]);
                }
            } catch (QException e) {
                System.out.println(e.status + " " + QException.ex[e.type] + ": " + e.msg);
            } catch (Exception e) {
                System.out.println("Default error: " + e.getMessage());
            }
        }
    }

    public execResult runSingleCommand(String state) {
        if(!state.contains(";"))
            return new execResult(0,"Wrong statement without semicolon" + "\n", 0);
        int index = state.indexOf(";");
        state = state.substring(0, index);
        String result = state.trim().replaceAll("\\s+", " ");
        String[] tokens = result.split(" ");

        try {
            if (tokens.length == 1 && tokens[0].equals("")){
                System.out.println("result");
                System.out.println(Arrays.toString(tokens));
                return new execResult(0,"No statement specified" + "\n", 0);
            }

            switch (tokens[0]) { //match keyword
                case "create":
                    if (tokens.length == 1)
                        return new execResult(0,  "Can't find create object" + "\n", 0);
                    return switch (tokens[1]) {
                        case "table" -> parse_create_table(result);
                        case "index" -> parse_create_index(result);
                        default -> new execResult(0, "Can't identify " + tokens[1] + "\n", 0);
                    };
                case "drop":
                    if (tokens.length == 1)
                        return new execResult(0,  "Can't find drop object" + "\n", 0);
                    return switch (tokens[1]) {
                        case "table" -> parse_drop_table(result);
                        case "index" -> parse_drop_index(result);
                        default -> new execResult(0, "Can't identify " + tokens[1] + "\n", 0);
                    };
                case "select":
                    return parse_select(result);
                case "insert":
                    return parse_insert(result);
                case "delete":
                    return parse_delete(result);
                case "execfile":
                    return parse_sql_file(result);
                case "show":
                    return parse_show(result);
                default:
                    return new execResult(0,  "Can't identify " + tokens[0] + "\n", 0);
            }
        } catch (QException e) {
            return new execResult(e.status, e.msg + "\n", 0);
        } catch (Exception e) {
            return new execResult(0, e.getMessage() + "\n", 0);
        }
    }

    private static execResult parse_show(String statement) throws Exception {
        String type = Utils.substring(statement, "show ", "").trim();
        if (type.equals("tables")) {
            CatalogManager.show_table();
            return new execResult(1, "Show Table Successfully" + "\n", 1);
        } else if (type.equals("indexes")) {
            CatalogManager.show_index();
            return new execResult(1, "Show Index Successfully" + "\n", 1);
        } else return new execResult(0,  "Can not find valid key word after 'show'!" + "\n", 0);
    }

    private execResult parse_create_table(String statement) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^create table", "").trim(); //skip create table keyword

        int startIndex, endIndex;
        if (statement.equals("")) //no statement after create table
            return new execResult(0,  "Must specify a table name" + "\n", 0);

        endIndex = statement.indexOf(" ");
        if (endIndex == -1)  //no statement after create table xxx
            return new execResult(0,  "Can't find attribute definition" + "\n", 0);

        String tableName = statement.substring(0, endIndex); //get table name
        startIndex = endIndex + 1; //start index of '('
        if (!statement.substring(startIndex).matches("^\\(.*\\)$"))  //check brackets
            return new execResult(0,  "Can't not find the definition brackets in table " + tableName + "\n", 0);

        int length;
        String[] attrParas, attrsDefine;
        String attrName, attrType, attrLength = "", primaryName = "";
        boolean attrUnique;
        Attribute attribute;
        Vector<Attribute> attrVec = new Vector<>();

        attrsDefine = statement.substring(startIndex + 1).split(","); //get each attribute definition
        for (int i = 0; i < attrsDefine.length; i++) { //for each attribute
            if (i == attrsDefine.length - 1) { //last line
                attrParas = attrsDefine[i].trim().substring(0, attrsDefine[i].length() - 1).split(" "); //remove last ')'
            } else {
                attrParas = attrsDefine[i].trim().split(" ");
            } //split each attribute in parameters: name, type,（length) (unique)

            if (attrParas[0].equals("")) { //empty
                return new execResult(0,  "Empty attribute in table " + tableName + "\n", 0);
            } else if (attrParas[0].equals("primary")) { //primary key definition
                if (attrParas.length != 3 || !attrParas[1].equals("key"))  //not as primary key xxxx
                    return new execResult(0,  "Error definition of primary key in table " + tableName + "\n", 0);
                if (!attrParas[2].matches("^\\(.*\\)$"))  //not as primary key (xxxx)
                    return new execResult(0,  "Error definition of primary key in table " + tableName + "\n", 0);
                if (!primaryName.equals("")) //already set primary key
                    return new execResult(0,  "Redefinition of primary key in table " + tableName + "\n", 0);

                primaryName = attrParas[2].substring(1, attrParas[2].length() - 1); //set primary key
            } else { //ordinary definition
                if (attrParas.length == 1)  //only attribute name
                    return new execResult(0,  "Incomplete definition in attribute " + attrParas[0] + "\n", 0);
                attrName = attrParas[0]; //get attribute name
                attrType = attrParas[1]; //get attribute type
                for (int j = 0; j < attrVec.size(); j++) { //check whether name redefines
                    if (attrName.equals(attrVec.get(j).attributeName))
                        return new execResult(0,  "Redefinition in attribute " + attrParas[0] + "\n", 0);
                }
                if (attrType.equals("int") || attrType.equals("float")) { //check type
                    endIndex = 2; //expected end index
                } else if (attrType.equals("char")) {
                    if (attrParas.length == 2)  //no char length
                        return new execResult(0,  "ust specify char length in " + attrParas[0] + "\n", 0);
                    if (!attrParas[2].matches("^\\(.*\\)$"))  //not in char (x) form
                        return new execResult(0,  "Wrong definition of char length in " + attrParas[0] + "\n", 0);

                    attrLength = attrParas[2].substring(1, attrParas[2].length() - 1); //get length
                    try {
                        length = Integer.parseInt(attrLength); //check the length
                    } catch (NumberFormatException e) {
                        return new execResult(0,  "The char length in " + attrParas[0] + " dosen't match a int type or overflow" + "\n", 0);
                    }
                    if (length < 1 || length > 255)
                        return new execResult(0,  "The char length in " + attrParas[0] + " must be in [1,255] " + "\n", 0);
                    endIndex = 3; //expected end index
                } else { //unmatched type
                    return new execResult(0,  "Error attribute type " + attrType + " in " + attrParas[0] + "\n", 0);
                }

                if (attrParas.length == endIndex) { //check unique constraint
                    attrUnique = false;
                } else if (attrParas.length == endIndex + 1 && attrParas[endIndex].equals("unique")) {  //unique
                    attrUnique = true;
                } else { //wrong definition
                    return new execResult(0,  "Error constraint definition in " + attrParas[0] + "\n", 0);
                }

                if (attrType.equals("char")) { //generate attribute
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()), Integer.parseInt(attrLength), attrUnique);
                } else {
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()), attrUnique);
                }
                attrVec.add(attribute);
            }
        }

        if (primaryName.equals(""))  //check whether set the primary key
            return new execResult(0,  "Not specified primary key in table " + tableName + "\n", 0);

        Table table = new Table(tableName, primaryName, attrVec); // create table
        api.create_table(tableName, table);
        /*For testing*/
//        System.out.println(1);
        return new execResult(1, "Create table " + tableName + " successfully" + "\n", 2);
    }

    private execResult parse_drop_table(String statement) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            return new execResult(0,  "Not specify table name" + "\n", 0);
        if (tokens.length != 3)
            return new execResult(0,  "Extra parameters in drop table" + "\n", 0);

        String tableName = tokens[2]; //get table name
        api.drop_table(tableName);
        return new execResult(1, "Drop table " + tableName + " successfully" + "\n", 3);
    }

    private execResult parse_create_index(String statement) throws Exception {
        statement = statement.replaceAll("\\s+", " ");
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.trim();

        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            return new execResult(0, "Not specify index name" + "\n", 0);

        String indexName = tokens[2]; //get index name
        if (tokens.length == 3 || !tokens[3].equals("on"))
            return new execResult(0,  "Must add keyword 'on' after index name " + indexName + "\n", 0);
        if (tokens.length == 4)
            return new execResult(0,  "Not specify table name" + "\n", 0);

        String tableName = tokens[4]; //get table name
        if (tokens.length == 5)
            return new execResult(0,  "Not specify attribute name in table " + tableName + "\n", 0);

        String attrName = tokens[5];
        if (!attrName.matches("^\\(.*\\)$"))  //not as (xxx) form
            return new execResult(0,  "Error in specifiy attribute name " + attrName + "\n", 0);

        attrName = attrName.substring(1, attrName.length() - 1); //extract attribute name
        if (tokens.length != 6)
            return new execResult(0,  "Extra parameters in create index" + "\n", 0);
        if (!CatalogManager.is_unique(tableName, attrName))
            return new execResult(1,  "Not a unique attribute" + "\n", 1);

        Index index = new Index(indexName, tableName, attrName);
        api.create_index(index);
        return new execResult(1, "Create index " + indexName + " successfully" + "\n", 1);
    }

    private execResult parse_drop_index(String statement) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            return new execResult(0,  "Not specify index name" + "\n", 0);
        if (tokens.length != 3)
            return new execResult(0,  "Extra parameters in drop index" + "\n", 0);

        String indexName = tokens[2]; //get table name
        api.drop_index(indexName);
        return new execResult(1, "Drop index " + indexName + " successfully" + "\n", 1);
    }

    private execResult parse_select(String statement) throws Exception {
        //select ... from ... where ...
        String attrStr = Utils.substring(statement, "select ", " from");
        String tabStr = Utils.substring(statement, "from ", " where");
        String conStr = Utils.substring(statement, "where ", "");
        Vector<Condition> conditions;
        Vector<String> attrNames;
        long startTime, endTime;
        startTime = System.currentTimeMillis();
        if (attrStr.equals(""))
            return new execResult(0,  "Can not find key word 'from' or lack of blank before from!\n", 0);
        if (attrStr.trim().equals("*")) {
            //select all attributes
            if (tabStr.equals("")) {  // select * from [];
                tabStr = Utils.substring(statement, "from ", "");
                Vector<TableRow> ret = api.select(tabStr, new Vector<>(), new Vector<>());
                endTime = System.currentTimeMillis();
                double usedTime = (endTime - startTime) / 1000.0;
                return new execResult(1, "Successful, Finished in " + usedTime + "s" + "\n" + Utils.print_rows(ret, tabStr) + "\n", 1);
            } else { //select * from [] where [];
                String[] conSet = conStr.split(" *and *");
                //get condition vector
                conditions = Utils.create_conditon(conSet);
                Vector<TableRow> ret = api.select(tabStr, new Vector<>(), conditions);
                endTime = System.currentTimeMillis();
                double usedTime = (endTime - startTime) / 1000.0;
                return new execResult(1, "Successful, Finished in " + usedTime + "s" + "\n" + Utils.print_rows(ret, tabStr) + "\n", 1);
            }
        } else {
            attrNames = Utils.convert(attrStr.split(" *, *")); //get attributes list
            if (tabStr.equals("")) {  //select [attr] from [];
                tabStr = Utils.substring(statement, "from ", "");
                Vector<TableRow> ret = api.select(tabStr, attrNames, new Vector<>());
                endTime = System.currentTimeMillis();
                double usedTime = (endTime - startTime) / 1000.0;
                return new execResult(1, "Successful, Finished in " + usedTime + "s" + "\n" + Utils.print_rows(ret, tabStr) + "\n", 1);
            } else { //select [attr] from [table] where
                String[] conSet = conStr.split(" *and *");
                //get condition vector
                conditions = Utils.create_conditon(conSet);
                Vector<TableRow> ret = api.select(tabStr, attrNames, conditions);
                endTime = System.currentTimeMillis();
                double usedTime = (endTime - startTime) / 1000.0;
                return new execResult(1, "Successful, Finished in " + usedTime + "s" + "\n" + Utils.print_rows(ret, tabStr) + "\n", 1);
            }
        }
    }

    private execResult parse_insert(String statement) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^insert", "").trim();  //skip insert keyword

        int startIndex, endIndex;
        if (statement.equals(""))
            return new execResult(0,  "Must add keyword 'into' after insert " + "\n", 0);

        endIndex = statement.indexOf(" "); //check into keyword
        if (endIndex == -1)
            return new execResult(0,  "Not specify the table name" + "\n", 0);
        if (!statement.substring(0, endIndex).equals("into"))
            return new execResult(0,  "Must add keyword 'into' after insert" + "\n", 0);

        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); //check table name
        if (endIndex == -1)
            return new execResult(0,  "Not specify the insert value" + "\n", 0);

        String tableName = statement.substring(startIndex, endIndex); //get table name
        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); //check values keyword
        if (endIndex == -1)
            return new execResult(0,  "Syntax error: Not specify the insert value" + "\n", 0);

        if (!statement.substring(startIndex, endIndex).equals("values"))
            return new execResult(0,  "Must add keyword 'values' after table " + tableName + "\n", 0);

        startIndex = endIndex + 1;
        if (!statement.substring(startIndex).matches("^\\(.*\\)$"))  //check brackets
            return new execResult(0,  "Can't not find the insert brackets in table " + tableName + "\n", 0);

        String[] valueParas = statement.substring(startIndex + 1).split(","); //get attribute tokens
        TableRow tableRow = new TableRow();

        for (int i = 0; i < valueParas.length; i++) {
            if (i == valueParas.length - 1)  //last attribute
                valueParas[i] = valueParas[i].substring(0, valueParas[i].length() - 1);
            if (valueParas[i].equals("")) //empty attribute
                return new execResult(0,  "Empty attribute value in insert value" + "\n", 0);
            if (valueParas[i].matches("^\".*\"$") || valueParas[i].matches("^\'.*\'$"))  // extract from '' or " "
                valueParas[i] = valueParas[i].substring(1, valueParas[i].length() - 1);
            tableRow.add_attribute_value(valueParas[i]); //add to table row
        }

        //Check unique attributes
        if (tableRow.get_attribute_size() != CatalogManager.get_attribute_num(tableName))
            return new execResult(0,  "Attribute number doesn't match" + "\n", 1);
        Vector<Attribute> attributes = CatalogManager.get_table(tableName).attributeVector;
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            if (attr.isUnique) {
                Condition cond = new Condition(attr.attributeName, "=", valueParas[i]);
                if (CatalogManager.is_index_key(tableName, attr.attributeName)) {
                    Index idx = CatalogManager.get_index(CatalogManager.get_index_name(tableName, attr.attributeName));
                    if (IndexManager.select(idx, cond).isEmpty())
                        continue;
                } else {
                    Vector<Condition> conditions = new Vector<>();
                    conditions.add(cond);
                    Vector<TableRow> res = RecordManager.select(tableName, conditions); //Supposed to be empty
                    if (res.isEmpty())
                        continue;
                }
                return new execResult(0,  "Duplicate unique key: " + attr.attributeName + "\n", 1);
            }
        }

        api.insert_row(tableName, tableRow);
        return new execResult(1, "Insert successfully" + "\n", 1);
    }

    private execResult parse_delete(String statement) throws Exception {
        //delete from [tabName] where []
        int num;
        String tabStr = Utils.substring(statement, "from ", " where").trim();
        String conStr = Utils.substring(statement, "where ", "").trim();
        Vector<Condition> conditions;
        Vector<String> attrNames;
        if (tabStr.equals("")) {  //delete from ...
            tabStr = Utils.substring(statement, "from ", "").trim();
            num = api.delete_row(tabStr, new Vector<>());
            return new execResult(1, "Query ok! " + num + " row(s) are deleted\n", 1);
        } else {  //delete from ... where ...
            String[] conSet = conStr.split(" *and *");
            //get condition vector
            conditions = Utils.create_conditon(conSet);
            num = api.delete_row(tabStr, conditions);
            return new execResult(1, "Query ok! " + num + " row(s) are deleted\n", 1);
        }
    }

    private void parse_quit(String statement, BufferedReader reader) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length != 1)
            throw new QException(0, 1001, "Extra parameters in quit");

        api.store();
        reader.close();
        System.out.println("Bye");
        System.exit(0);
    }

    private execResult parse_sql_file(String statement) throws Exception {
        execFile++;
        String[] tokens = statement.split(" ");
        if (tokens.length != 2)
            return new execResult(0,  "Extra parameters in sql file execution", 0);

        String fileName = tokens[1];
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(fileName));
//            if (nestLock)  //first enter in sql file execution
//                throw new QException(0, 1102, "Can't use nested file execution");
//            nestLock = true; //lock, avoid nested execution
            interpret(fileReader);
        } catch (FileNotFoundException e) {
            return new execResult(1,  "Can't find the file", 1);
        } catch (IOException e) {
            return new execResult(1,  "IO exception occurs", 1);
        } finally {
            execFile--;
//            nestLock = false; //unlock
        }
        return new execResult(0,  "Exec file Successfully", 0);
    }
}

class Utils {

    public static final int NONEXIST = -1;
    public static final String[] OPERATOR = {"<>", "<=", ">=", "=", "<", ">"};

    public static String substring(String str, String start, String end) {
        String regex = start + "(.*)" + end;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) return matcher.group(1);
        else return "";
    }

    public static <T> Vector<T> convert(T[] array) {
        Vector<T> v = new Vector<>();
        for (int i = 0; i < array.length; i++) v.add(array[i]);
        return v;
    }

    //ab <> 'c' | cab ="fabd"  | k=5  | char= '53' | int = 2
    public static Vector<Condition> create_conditon(String[] conSet) throws Exception {
        Vector<Condition> c = new Vector<>();
        for (int i = 0; i < conSet.length; i++) {
            int index = contains(conSet[i], OPERATOR);
            if (index == NONEXIST) throw new Exception("Syntax error: Invalid conditions " + conSet[i]);
            String attr = substring(conSet[i], "", OPERATOR[index]).trim();
            String value = substring(conSet[i], OPERATOR[index], "").trim().replace("\'", "").replace("\"", "");
            c.add(new Condition(attr, OPERATOR[index], value));
        }
        return c;
    }

    public static boolean check_type(String attr, boolean flag) {
        return true;
    }

    public static int contains(String str, String[] reg) {
        for (int i = 0; i < reg.length; i++) {
            if (str.contains(reg[i])) return i;
        }
        return NONEXIST;
    }

    public static void printRow(TableRow row) {
        for (int i = 0; i < row.get_attribute_size(); i++) {
            System.out.print(row.get_attribute_value(i) + "\t");
        }
        System.out.println();
    }

    public static int get_max_attr_length(Vector<TableRow> tab, int index) {
        int len = 0;
        for (int i = 0; i < tab.size(); i++) {
            int v = tab.get(i).get_attribute_value(index).length();
            len = v > len ? v : len;
        }
        return len;
    }

    public static String print_rows(Vector<TableRow> tab, String tabName) {
        StringBuilder res_val = new StringBuilder();
        if (tab.size() == 0) {
            res_val = new StringBuilder("Query ok! 0 rows are selected").append("\n");
            return res_val.toString();
        }
        int attrSize = tab.get(0).get_attribute_size();
        int cnt = 0;
        Vector<Integer> v = new Vector<>(attrSize);
        for (int j = 0; j < attrSize; j++) {
            int len = get_max_attr_length(tab, j);
            String attrName = CatalogManager.get_attribute_name(tabName, j);
            if (attrName.length() > len) len = attrName.length();
            v.add(len);
            String format = "|%-" + len + "s";
            String temp = String.format("|%-" + len + "s", attrName);
            res_val.append(temp);
            cnt = cnt + len + 1;
        }
        cnt++;
        res_val.append("|").append("\n");
        for (int i = 0; i < cnt; i++) res_val.append("-");
        res_val.append("\n");
        for (int i = 0; i < tab.size(); i++) {
            TableRow row = tab.get(i);
            for (int j = 0; j < attrSize; j++) {
                String format = "|%-" + v.get(j) + "s";
                String temp = String.format(format, row.get_attribute_value(j));
                res_val.append(temp);
            }
            res_val.append("|\n");
        }
        res_val = new StringBuilder(res_val + "Query ok! " + tab.size() + " rows are selected" + "\n");
        return res_val.toString();
    }
}
