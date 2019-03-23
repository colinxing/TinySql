import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;

import java.util.ArrayList;
import java.util.List;

// Create part, redirect from operation
public class Create extends Operation {
    @Override
    public Argument execute(Argument argument) {
        ArrayList<String> field_name = new ArrayList<String>();
        ArrayList<FieldType> field_type = new ArrayList<FieldType>();
        Statement table = argument.arguments.get(0);
        if(!table.getContent().equalsIgnoreCase("TableName"))
            throw new RuntimeException("ERROR: not TABLE statement!");
        String relation_name = table.getSubstatement().get(0).getContent();

        boolean params = setParams(field_name, field_type, argument);
        if(!params)
            return null;

        // build schema
        Schema schema = new Schema(field_name, field_type);
        if(argument.schema_manager == null)
            System.out.println("ERROR: storageManager creates schema Manager failed!");

        Relation newRelation = argument.schema_manager.createRelation(relation_name, schema);
        
        //handle error
        if (newRelation != null) {
            System.out.println("Congratulation! You have successfully created table-------->" + relation_name);
        }else {
            System.out.println("Sorry, you have failed to created table--------->" + relation_name);
        }
        return null;
    }

    boolean setParams(ArrayList<String> fieldName, ArrayList<FieldType> fieldType, Argument argument)
    {
        List<Statement> col_details = argument.arguments.get(1).getSubstatement();
        for (Statement tmp_statement: col_details) {

            assert tmp_statement.getContent().equalsIgnoreCase("CreateField") : "ERROR: not CreateField statement!";
            fieldName.add(tmp_statement.getSubstatement().get(0).getSubstatement().get(0).getContent());
            String type = tmp_statement.getSubstatement().get(1).getSubstatement().get(0).getContent();
            if(type.equals("INT")) {
                fieldType.add(FieldType.INT);
            } else if (type.equals("STR20")) {
                fieldType.add(FieldType.STR20);
            } else {
                System.out.println("ERROR: invalid attr type!");
                return false;
            }
        }
        return true;
    }
}

