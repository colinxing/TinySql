import java.util.*;

public class QueryParser {
    
    HashMap<String, Integer> operator_priority;
    public QueryParser() {
        operator_priority = new HashMap<String, Integer>();
        operator_priority.put("OR", 0);
        operator_priority.put("AND",1);
        operator_priority.put("=", 2);
        operator_priority.put(">", 2);
        operator_priority.put("<", 2);
        operator_priority.put("+", 3);
        operator_priority.put("-", 3);
        operator_priority.put("*", 4);
        operator_priority.put("/", 4);
    }
    public Statement parse(String query) {
        System.out.println("Processing '"+ query + "'");
        return parse(query.split(" "), "START");
    }

    public Statement parse(String[] query_array, String key) {
        Statement statement = null;

        if (key.equalsIgnoreCase("START")) {
            if (query_array[0].equalsIgnoreCase("CREATE")) {
                statement = parse(query_array, "CREATE");
            }

            if (query_array[0].equalsIgnoreCase("INSERT")) {
                statement = parse(query_array, "INSERT");
            }
            
            if (query_array[0].equalsIgnoreCase("SELECT")) {
                statement = parse(query_array, "SELECT");
            }

            if (query_array[0].equalsIgnoreCase("DELETE")) {
                statement = parse(query_array, "DELETE");
            }


            if (query_array[0].equalsIgnoreCase("DROP")) {
                statement = new Statement("DROP");
                statement.getSubstatement().add(leaf(query_array[2],"TableName"));
            }
        }

        //CREATE TableName course (sid INT, homework INT, project INT, exam INT, grade STR20)
        if (key.equalsIgnoreCase("CREATE")) {
            Statement tmp_statement = new Statement(key);
            tmp_statement.getSubstatement().add(leaf(query_array[2],  "TableName"));
            tmp_statement.getSubstatement().add(parse(Arrays.copyOfRange(query_array, 3, query_array.length), "CreateField"));
            statement = tmp_statement;
        }

        //(sid INT, homework INT, project INT, exam INT, grade STR20)
        if (key.equalsIgnoreCase("CreateField")) {
            Statement tmp_statement =  new Statement(key);
            if (query_array.length % 2 != 0) {
                System.out.println("ERROR! check attribute list format");
            }

            for (int i = 0; i < query_array.length / 2; i++) {
                tmp_statement.getSubstatement().add(parse(Arrays.copyOfRange(query_array, 2*i, 2*i+2), "FieldInfo"));
            }
            statement = tmp_statement;
        }

        //(sid INT, //homework INT, //project INT, //exam INT, //grade STR20)
        if (key.equalsIgnoreCase("FieldInfo")) {
            Statement tmp_statement = new Statement(key);
            String attr_name = query_array[0];
            String attr_type = query_array[1];

            if (attr_name.charAt(0) == '(' || attr_name.charAt(0) == ',') {
                attr_name = attr_name.substring(1);
            }
            if (attr_name.charAt(attr_name.length()-1) == ',' || attr_name.charAt(attr_name.length()-1) == ')') {
                attr_name = attr_name.substring(0, attr_name.length()-1);
            }
            if (attr_type.charAt(attr_type.length()-1) == ',' || attr_type.charAt(attr_type.length()-1) == ')') {
                attr_type= attr_type.substring(0, attr_type.length()-1);
            }

            tmp_statement.getSubstatement().add(leaf(attr_name, "FieldName"));
            tmp_statement.getSubstatement().add(leaf(attr_type, "FieldType"));

            statement = tmp_statement;
        }

        if (key.equalsIgnoreCase("INSERT")) {
            int pos_value = 0;
            int pos_select = 0;
            //get position of key word "VALUES" and "SELECT"
            for (int i = 0; i < query_array.length; i++) {
                if (query_array[i].equalsIgnoreCase("VALUES")) pos_value = i;
                if (query_array[i].equalsIgnoreCase("SELECT")) pos_select = i;
            }

            //parse table, column and values
            if (pos_value > 0) {
                Statement tmp_statement = new Statement(key);
                tmp_statement.getSubstatement().add(leaf(query_array[2], "TableName"));
                tmp_statement.getSubstatement().add(parse(Arrays.copyOfRange(query_array, 3, pos_value), "Fields"));
                tmp_statement.getSubstatement().add(parse(Arrays.copyOfRange(query_array, pos_value+1, query_array.length), "VALUES"));
                statement = tmp_statement;
            }

            if (pos_select > 0) {
                Statement tmp_statement =  new Statement(key);
                tmp_statement.getSubstatement().add(leaf(query_array[2],"TableName"));
                tmp_statement.getSubstatement().add(parse(Arrays.copyOfRange(query_array, 3, pos_select), "Fields"));
                tmp_statement.getSubstatement().add(parse(Arrays.copyOfRange(query_array,pos_select,query_array.length),"SELECT"));
                statement = tmp_statement;
            }

        }

        if (key.equalsIgnoreCase("VALUES"))
        {
                Statement tmp_statement = new Statement(key);
                for (String elem_in_query: query_array)
                {
                    String item = trim(elem_in_query);
                    tmp_statement.getSubstatement().add(leaf(item,"Value"));
                }
                statement = tmp_statement;
        }

        if (key.equalsIgnoreCase("SELECT")) {
            Statement tmp_statement = new Statement(key);
            int pos_from = 0, pos_where = 0, pos_order = 0;
            int pos_current=1;
            while(pos_current < query_array.length)
            {
                if (query_array[pos_current].equalsIgnoreCase("FROM"))
                {
                    pos_from = pos_current;
                    Statement tmp_fields = parse(Arrays.copyOfRange(query_array,1,pos_from),"Fields");
                    tmp_statement.getSubstatement().add(tmp_fields);
                }
                else if (query_array[pos_current].equalsIgnoreCase("WHERE"))
                {
                    pos_where = pos_current;
                    Statement tmp_from = parse(Arrays.copyOfRange(query_array, pos_from+1,pos_where),"FROM");
                    tmp_statement.getSubstatement().add(tmp_from);
                }
                else if (query_array[pos_current].equalsIgnoreCase("ORDER"))
                {
                    pos_order = pos_current;
                    if (pos_where != 0)
                    {
                        Statement tmp_where = parse(Arrays.copyOfRange(query_array, pos_where+1, pos_order),"WHERE");
                        tmp_statement.getSubstatement().add(tmp_where);
                    }
                    else if (pos_where == 0)
                    {
                        Statement tmp_from = parse(Arrays.copyOfRange(query_array, pos_from+1, pos_order),"FROM");
                        tmp_statement.getSubstatement().add(tmp_from);
                    }
                    Statement tmp_order = parse(Arrays.copyOfRange(query_array, pos_order+2, query_array.length),"ORDER");
                        tmp_statement.getSubstatement().add(tmp_order);
                }
                pos_current++;
            }
            if (pos_order == 0 && pos_where !=0)
            {
                 Statement tmp_where = parse(Arrays.copyOfRange(query_array, pos_where+1, query_array.length),"WHERE");
                 tmp_statement.getSubstatement().add(tmp_where);
            }
            if (pos_order == 0 && pos_where == 0)
            {
                Statement tmp_from = parse(Arrays.copyOfRange(query_array, pos_from+1, query_array.length),"FROM");
                tmp_statement.getSubstatement().add(tmp_from);
            }
            statement = tmp_statement;
        }

        if (key.equalsIgnoreCase("FROM")) {
            Statement tmp_statement = new Statement(key);
            int pos_current = 0;
            for (pos_current = 0; pos_current < query_array.length;pos_current++)
            {
                String elem_in_query = query_array[pos_current];
                if(elem_in_query.charAt(elem_in_query.length()-1)==',')
                {
                    elem_in_query = elem_in_query.substring(0,elem_in_query.length()-1);
                }
                tmp_statement.getSubstatement().add(leaf(elem_in_query,"TableName"));
            }
            statement = tmp_statement;
        }

        if (key.equalsIgnoreCase("WHERE")) {
            Statement tmp_statement = new Statement("EXPRESSION");
            //System.out.println("condition returns: " + condition(query_array).getContent());
            tmp_statement.getSubstatement().add(condition(query_array));

            statement = tmp_statement;
        }

        if(key.equalsIgnoreCase("ORDER")) {
            Statement tmp_statement = new Statement("ORDER");
            tmp_statement.getSubstatement().add(leaf(query_array[0],"FieldName"));
            statement = tmp_statement;
        }

        if(key.equalsIgnoreCase("Fields")) {
            Statement tmp_statement = new Statement(key);
            if (query_array[0].equalsIgnoreCase("DISTINCT")) {
                tmp_statement.getSubstatement().add( new Statement("DISTINCT"));
                Statement attr = tmp_statement.getSubstatement().get(0);
                for (int i=1; i < query_array.length; i++)
                {
                    String elem_in_query = query_array[i];
                    if (elem_in_query.length()>0)
                    {
                        attr.getSubstatement().add(leaf(elem_in_query.charAt(elem_in_query.length()-1)==','?elem_in_query.substring(0,elem_in_query.length()-1):elem_in_query,"FieldName"));

                    }
                }
            }
            else
            {
                for (String elem_in_query : query_array) {
                    if(elem_in_query.length() > 0) {
                        String item = elem_in_query;
                         //System.out.println("item in query = "+ item + " elem length:" + elem_in_query.length()+ "query_array"+query_array.length);
                        if(item.charAt(0) == '(')
                        {
                            item = item.substring(1, item.length());
                        }
                        if(item.charAt(item.length()-1)==')'||item.charAt(item.length()-1)==',')
                        {
                            item = item.substring(0, item.length()-1);
                        }
                        tmp_statement.getSubstatement().add(leaf(item, "FieldName"));
                    }
                }
            }
            statement = tmp_statement;
        }

        if(key.equalsIgnoreCase("DROP"))
        {
            Statement tmp_statement = new Statement(key);
            tmp_statement.getSubstatement().add(leaf(query_array[2],"TableName"));
            statement = tmp_statement;
        }

        if(key.equalsIgnoreCase("DELETE"))
        {
            Statement tmp_statement = new Statement("DELETE");
            String table = query_array[2];
            tmp_statement.getSubstatement().add(leaf(table,"TableName"));
            if(query_array.length > 3 && query_array[3].equalsIgnoreCase("WHERE"))
            {
                tmp_statement.getSubstatement().add(parse(Arrays.copyOfRange(query_array, 4, query_array.length), "WHERE"));
            }
            statement = tmp_statement;
        }
        return statement;
    }


    public Statement leaf(String str, String key) {
        if (key.equalsIgnoreCase("FieldName")) {
            Statement tmp_statement = new Statement("FieldName");
            String[] name = str.split("\\.");
            for (String elem_in_name : name) {
                tmp_statement.getSubstatement().add(new Statement(elem_in_name, true));
            }
            return tmp_statement;
        }
        else if(key.equalsIgnoreCase("TableName"))
        {
            Statement tmp_statement = new Statement("TableName");
            tmp_statement.getSubstatement().add(new Statement(str,true));
            return tmp_statement;
        }
        else {
            //System.out.println("pending...");
            Statement tmp_statement = new Statement(key);
            tmp_statement.getSubstatement().add(new Statement(str, true));
            return tmp_statement;
        }

    }

    public Statement condition(String[] query_all)
    {
        Stack<Statement> query_list_stack = new Stack<Statement>();
        int i = 0;
        for (i = 0; i < query_all.length; i++)
        {
            if (operator_priority.containsKey(query_all[i]))
            {
                if(query_list_stack.size() >= 3)
                {
                    Statement last = query_list_stack.pop();
                    if(operator_priority.get(query_all[i]) >= operator_priority.get(query_list_stack.peek().getContent()))
                    {
                        query_list_stack.push(last);
                        query_list_stack.push(new Statement(query_all[i]));
                    }
                    else
                    {
                        while (query_list_stack.size()>0 && operator_priority.get(query_list_stack.peek().getContent())>operator_priority.get(query_all[i]))
                        {
                            Statement operation = query_list_stack.pop();
                            
                            Statement operation2 = query_list_stack.pop();
                            operation.getSubstatement().add(operation2);
                            operation.getSubstatement().add(last);
                            last = operation;
                        }
                        
                        query_list_stack.push(last);
                        query_list_stack.push(new Statement(query_all[i]));
                    }
                }
                else
                {
                    query_list_stack.push(new Statement(query_all[i]));
                }
            }
            else if(integer(query_all[i]))
            {
                query_list_stack.push(leaf(query_all[i], "INT"));
            }
            else if(query_all[i].charAt(0) == '"')
            {
                query_list_stack.push(leaf(query_all[i].substring(1, query_all[i].length()-1),"STRING"));
            }
            else if(query_all[i].charAt(0) == '(')
            {
                String[] sub_where_operation = new String[3];
                sub_where_operation[0]=trim(query_all[i]);
                sub_where_operation[1]=query_all[i+1];
                sub_where_operation[2]=trim(query_all[i+2]);
                query_list_stack.push(condition(sub_where_operation));
                i=i+2;
            }
            else
            {
                query_list_stack.push(leaf(query_all[i],"FieldName"));
            }
        }
        
        if(query_list_stack.size() >= 3)
        {
            Statement current_elem = query_list_stack.pop();
            
            while(query_list_stack.size() >= 2)
            {
                Statement operation = query_list_stack.pop();
                operation.getSubstatement().add(query_list_stack.pop());
                operation.getSubstatement().add(current_elem);
                current_elem = operation;
            }
            return current_elem;
        }
        else
        {
            return query_list_stack.peek();
        }
    }
    
    public String trim(String substring)
    {
        String str = substring;
        if(str.length() == 0) return null;
        if (str.charAt(0)=='(') str = str.substring(1);
        if (str.charAt(0)=='"') str = str.substring(1);
        if (str.charAt(str.length()-1)==')') str = str.substring(0, str.length()-1);
        if (str.charAt(str.length()-1)==',') str = str.substring(0, str.length()-1);
        if (str.charAt(str.length()-1)=='"') str = str.substring(0, str.length()-1);
        return str;
    }
    
    public static boolean integer(String substring)
    {
        try 
        {
            Integer.parseInt(substring);
            return true;
        }
        catch (NumberFormatException err) 
        {
            return false;
        }
    }
}

