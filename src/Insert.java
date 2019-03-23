import storageManager.*;

import java.util.*;

// Insert part, redirect from operation

public class Insert extends Operation {
    @Override
    public Argument execute(Argument argument) {
        //test if argument contains command
        if(argument.schema_manager != null && argument.memory != null) {
            //Detection of statements
            List<Statement> list = argument.arguments;
            List<Statement> col_list = null;
            String relation_name = null;

            for (Statement sub_statement : list) {
                String subContent = sub_statement.getContent();
                // pass table name from command to relation
                if (subContent.equalsIgnoreCase("TableName")) {
                    relation_name = sub_statement.getSubstatement().get(0).getContent();
                } else if (subContent.equalsIgnoreCase("Fields")) {
                    // pass list of statment name from command to relation
                    col_list = sub_statement.getSubstatement();
                } else if (subContent.equalsIgnoreCase("VALUES")) {
                    // pass relation name from command to relation
                    Relation relation = argument.schema_manager.getRelation(relation_name);
                    Tuple tupleToCreat = relation.createTuple();
                    // set Tuple
                    buildTuple(tupleToCreat, col_list, sub_statement, relation, argument);
                } else if (subContent.equalsIgnoreCase("SELECT")) {
                    // if an insert command contains selectSELECT * FROM course
                    Relation tempRelation = ParserHelper.selectHandler(argument.schema_manager, argument.memory, "", 1);
                    String[] tempFields = {"sid", "homework", "project", "exam", "grade"};
                    ArrayList<String> tempList = new ArrayList<>(Arrays.asList(tempFields));
                    ParserHelper.insertFromSel(argument.schema_manager, argument.memory, argument.schema_manager.getRelation(relation_name), tempList, tempRelation);
                }
            }
            System.out.println("INSERT COMPLETE!");
            return null;
        } else {
            throw new RuntimeException("Wrong at insert");
        }
    }

    private void buildTuple(Tuple T, List<Statement> L, Statement cur, Relation Rel, Argument Arg) {

        if (!L.isEmpty()) {
            int tmp = 0;

            for (Statement tmp_statement : L) {
                if(tmp_statement.getContent().equalsIgnoreCase("FieldName")
                        && cur.getSubstatement().get(tmp).getContent().equalsIgnoreCase("Value")
                        && tmp_statement.getSubstatement().size() == 1
                        && T.getSchema().getFieldType(tmp_statement.getSubstatement().get(0).getContent()) != null) {

                    String content_value = cur.getSubstatement().get(tmp).getSubstatement().get(0).getContent();
                    String tmp_sub_content = tmp_statement.getSubstatement().get(0).getContent();

                    if (T.getSchema().getFieldType(tmp_sub_content).equals(FieldType.INT)) {
                        int temp = -1;
                        try {
                           temp = Integer.parseInt(content_value);
                        }catch(Exception e) {}
                        T.setField(tmp_sub_content, temp);
                    } else {
                        T.setField(tmp_sub_content, content_value);
                    }
                    tmp += 1;
                }
            }
            ParserHelper.appendTuple(Rel, Arg.memory, 0, T);
        }

    }

}
