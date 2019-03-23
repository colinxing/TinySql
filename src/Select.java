import storageManager.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

// Operations insert part here
public class Select extends Operation {
    @Override

    public Argument execute(Argument argument) {
        try {
            argument.fw = new FileWriter("Result.txt", true);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        HashMap<String, List<Statement>> map = new HashMap<String, List<Statement>>();
        Statement column = null, from = null, order = null;
        Expression expression = null;
        SchemaManager schema_manager = argument.schema_manager;
        MainMemory memory = argument.memory;

        for (Statement statement : argument.arguments) {
            if (statement.getContent().equalsIgnoreCase("Fields")) column = statement;
            if (statement.getContent().equalsIgnoreCase("FROM")) from = statement;
            if (statement.getContent().equalsIgnoreCase("EXPRESSION")) expression = new Expression(statement);
            if (statement.getContent().equalsIgnoreCase("ORDER")) order = statement;
        }
        assert from != null;
        assert column != null;

        Statement columns = column;
        if (columns.getSubstatement().get(0).getContent().equalsIgnoreCase("DISTINCT")) {
            columns = columns.getSubstatement().get(0);
        }
        ArrayList<String> fieldList = new ArrayList<>();
        int idx = 0;
        while (idx < columns.getSubstatement().size()) {
            Statement s = columns.getSubstatement().get(idx);
            if (s.getContent() != "FieldName") {
                throw new RuntimeException("Error: please check you 'Select' query format!");
            }
            int j = 0;
            StringBuilder fieldName = new StringBuilder();
            while (j < s.getSubstatement().size()) {
                Statement subfield = s.getSubstatement().get(j);
                fieldName.append(subfield.getContent() + '.');
                j++;
            }
            fieldName.deleteCharAt(fieldName.length() - 1);
            fieldList.add(fieldName.toString());
            idx++;
        }

        if (from.getSubstatement().size() == 1) {
            // We will judge if the format of the "Select" query is true first of all
            if (!from.getSubstatement().get(0).getContent().equalsIgnoreCase("TableName")) {
                throw new RuntimeException("Error: please check you 'Select' query format!");
            }
            String relationName = from.getSubstatement().get(0).getSubstatement().get(0).getContent();
            Relation relation = schema_manager.getRelation(relationName);

            if (relation.getNumOfBlocks() <= memory.getMemorySize()) {
                // All blocks of the relation could be fit into main memory
                relation.getBlocks(0, 0, relation.getNumOfBlocks());
                ArrayList<Tuple> tuples;
                tuples = memory.getTuples(0, relation.getNumOfBlocks());
                if (expression != null) {
                    ArrayList<Tuple> where = new ArrayList<>();
                    for (Tuple tuple : tuples) {
                        if (expression.evaluateBoolean(tuple)) {
                            where.add(tuple);
                        }
                    }
                    tuples = where;
                }

                if (order != null || column.getSubstatement().get(0).getContent().equalsIgnoreCase("DISTINCT")) {
                    //Make them in order
                    if (tuples.size() == 0) {
                        System.out.println("Empty Table");
                        return null;
                    }
                    Algorithms.sortInMemory(tuples, order == null ? null : order.getSubstatement().get(0).getSubstatement().get(0).getContent());

                    if (column.getSubstatement().get(0).getContent().equalsIgnoreCase("DISTINCT")) {
                        Algorithms.removeDuplicate(tuples, fieldList);
                    }
                }

                try (BufferedWriter bw = new BufferedWriter(argument.fw)) {
                    System.out.println("--------------------------------");
                    String content = "--------------------------------\n";
                    bw.write(content);
                    if (column.getSubstatement().get(0).getSubstatement().get(0).getContent().equals("*")) {
                        //System.out.println(tuples.get(0).toString(true));
                        try {
                            for (String s : tuples.get(0).getSchema().getFieldNames()) {
                                System.out.print(s + "  ");
                                bw.write(s + "  ");
                            }
                            System.out.println();
                            bw.write("\n");
                            for (Tuple t : tuples) {
                                System.out.println(t);
                                String tuplestring = t.toString(false);
                                bw.write(tuplestring + "\n");
                            }
                        } catch (Exception exp) {
                            System.out.println("No tuples");
                            bw.write("No tuples" + "\n");
                        }
                    } else {
                        if (column.getSubstatement().get(0).getContent().equalsIgnoreCase("DISTINCT")) { // with distinct
                            List<Statement> li = column.getSubstatement().get(0).getSubstatement();
                            cycle(li, bw);
                        } else { // without distinct
                            List<Statement> li = column.getSubstatement();
                            cycle(li, bw);
                        }

                        System.out.println();
                        bw.write("\n");

                        for (Tuple t : tuples) {
                            if (column.getSubstatement().get(0).getContent().equalsIgnoreCase("DISTINCT")) { //has distinct
                                cycle1(t, column.getSubstatement().get(0).getSubstatement(), bw);
                            } else { //without distinct
                                cycle1(t, column.getSubstatement(), bw);
                            }
                            System.out.println();
                            bw.write("\n");
                        }
                    }
                    System.out.println("--------------------------------");

                    bw.write("--------------------------------\n\n\n");

                } catch (IOException e) {
                }
            }

            else {
                ArrayList<String> fields = new ArrayList<>();
                boolean distinct = false;
                if (column.getSubstatement().get(0).getContent().equalsIgnoreCase("DISTINCT")) {
                    distinct = true;
                    column = column.getSubstatement().get(0);
                }

                int scan = 0;
                while(scan < column.getSubstatement().size()){
                    Statement field = column.getSubstatement().get(scan);
                    if (field.getSubstatement().size() == 1) { //attr
                        fields.add(field.getSubstatement().get(0).getContent());
                    } else { //table.attr
                        fields.add(field.getSubstatement().get(0).getContent() + "." + field.getSubstatement().get(1).getContent());
                    }
                    scan++;
                }

                if (order == null && !distinct) {
                    // basic select operation with/without where.
//                    System.out.println("Basic select operation");
                    regularSelect(memory, schema_manager, relationName, fields, expression);
                } else {
                    //select from one table in order/distinct
                    System.out.println("Select from one table in order/distinct");
                    String orderField = order == null ? null : order.getSubstatement().get(0).getSubstatement().get(0).getContent();
                    superiorSelect(memory, schema_manager, relationName, fields, expression, orderField, distinct);
                }
            }

        }

        else {
            boolean distinct = false;
            if (column.getSubstatement().get(0).getContent().equalsIgnoreCase("DISTINCT")) {
                distinct = true;
                column.setSubstatement(column.getSubstatement().get(0).getSubstatement());
            }

            if (expression != null && expression.exp_statement.getSubstatement().get(0).getContent().equals("=")) {
                Statement eqs = expression.exp_statement.getSubstatement().get(0);
                if (eqs.getSubstatement().get(0).getContent().equalsIgnoreCase("FieldName")
                        && eqs.getSubstatement().get(1).getContent().equalsIgnoreCase("FieldName")) {
                    String table1 = eqs.getSubstatement().get(0).getSubstatement().get(0).getContent();
                    String table2 = eqs.getSubstatement().get(1).getSubstatement().get(0).getContent();
                    String field0 = eqs.getSubstatement().get(0).getSubstatement().get(1).getContent();
                    String field1 = eqs.getSubstatement().get(1).getSubstatement().get(1).getContent();
                    if (field0.equals(field1)) {
                        System.out.println("Natural join optimization is applied");
                        Relation r = ParserHelper.executeNaturalJoin(schema_manager, memory, table1, table2, field0, 1);
                        Algorithms.mergeField(expression.exp_statement);
                        Algorithms.mergeField(column);
                        ArrayList<String> fields = new ArrayList<>();

                        for (Statement ids : column.getSubstatement()) {
                            if (ids.getSubstatement().size() == 1) {// attr
                                fields.add(ids.getSubstatement().get(0).getContent());
                            } else {//table.attr
                                fields.add(ids.getSubstatement().get(0).getContent() + "." + ids.getSubstatement().get(1).getContent());
                            }
                        }
                        if (!distinct && order == null) {
                            ParserHelper.filter(schema_manager, memory, r, expression, fields, 0);
                            return null;
                        }
                        Relation ra = ParserHelper.filter(schema_manager, memory, r, expression, fields, 1);//get tuple
                        if (distinct && order == null) {
                            if (fields.get(0).equals("*")) {
                                fields = ra.getSchema().getFieldNames();
                            }
                            ParserHelper.executeDistinct(schema_manager, memory, ra, fields, 0);
                            return null;
                        }
                        if (!distinct && order != null) {
                            Algorithms.mergeField(order);
                            ArrayList<String> orderField = new ArrayList<>();

                            if (order.getSubstatement().get(0).getSubstatement().size() == 1) {// attr
                                orderField.add(order.getSubstatement().get(0).getSubstatement().get(0).getContent());
                            } else {//table.attr
                                orderField.add(order.getSubstatement().get(0).getSubstatement().get(0).getContent() + "." + order.getSubstatement().get(0).getSubstatement().get(1).getContent());
                            }


                            ParserHelper.executeOrder(schema_manager, memory, ra, orderField, 0);
                            return null;
                        }
                        if (distinct && order != null) {
                            if (fields.get(0).equals("*")) {
                                //System.out.print( "274 ");
                                fields = ra.getSchema().getFieldNames();
                            }
                            Algorithms.mergeField(order);
                            ArrayList<String> orderField = new ArrayList<>();
                            orderField.add(order.getSubstatement().get(0).getSubstatement().get(0).getContent());
                            ParserHelper.executeOrder(schema_manager, memory, ParserHelper.executeDistinct(schema_manager, memory, ra, fields, 1), orderField, 0);
                            return null;
                        }

                        return null;
                    }
                }
            }
            System.out.println("Execute Select in multi-relation");


            ArrayList<String> relationList = new ArrayList<>();
            for (Statement relation : from.getSubstatement()) {
                assert relation.getContent().equalsIgnoreCase("TableName");
                relationList.add(relation.getSubstatement().get(0).getContent());
            }

            if (!distinct && order == null && column.getSubstatement().get(0).getSubstatement().get(0).getContent().equals("*") && expression == null) {
                MultiRelationCrossJoin(schema_manager, memory, relationList, 0);
                return null;
            }
            //three tables
            if (relationList.size() == 3) {
                //run a DP algorithm to determine the order of join.
                int memsize = memory.getMemorySize();
                HashMap<Set<String>, CrossRelation> singleRelation = new HashMap<>();
                for (String name : relationList) {
                    HashSet<String> set = new HashSet<>();
                    set.add(name);
                    Relation relation = schema_manager.getRelation(name);
                    CrossRelation temp = new CrossRelation(set, relation.getNumOfBlocks(), relation.getNumOfTuples());
                    temp.cost = relation.getNumOfBlocks();
                    temp.fieldNum = relation.getSchema().getNumOfFields();
                    singleRelation.put(set, temp);
                }
                //List of HashMap should be DP table
                List<HashMap<Set<String>, CrossRelation>> costRelationList = new ArrayList<>();
                costRelationList.add(singleRelation);
                for (int i = 1; i < relationList.size(); i++) {
                    costRelationList.add(new HashMap<Set<String>, CrossRelation>());
                }

                Set<String> finalGoal = new HashSet<>(relationList);
                CrossRelation cr = Algorithms.findOptimal(costRelationList, finalGoal, memsize);
                Algorithms.travesal(cr, 0);

                //get the join attrs
                Statement eql_statement = expression.exp_statement.getSubstatement().get(0).getSubstatement().get(0);
                String joinR1 = eql_statement.getSubstatement().get(0).getSubstatement().get(0).getContent();
                String joinF1 = eql_statement.getSubstatement().get(0).getSubstatement().get(1).getContent();
                String joinR2 = eql_statement.getSubstatement().get(1).getSubstatement().get(0).getContent();
                String joinF2 = eql_statement.getSubstatement().get(1).getSubstatement().get(1).getContent();

                System.out.println("Start to join first two tables");
                Relation firstJoinR = ParserHelper.executeNaturalJoin(schema_manager, memory, joinR1, joinR2, joinF1, 1);

                String dub_field_names = firstJoinR.getSchema().fieldNamesToString();

                Statement second_eql_statement = expression.exp_statement.getSubstatement().get(0).getSubstatement().get(1).getSubstatement().get(0);
                String joinR3 = second_eql_statement.getSubstatement().get(1).getSubstatement().get(0).getContent();
                String joinF3 = second_eql_statement.getSubstatement().get(1).getSubstatement().get(1).getContent();

                System.out.println("Start to join the intermediate and third tables");
                Relation secondJoinR = ParserHelper.executeNaturalJoin(schema_manager, memory, firstJoinR.getRelationName(), joinR3, joinF3, 1);

                //selection on third expression
                Statement tmpThirdEql = new Statement("=");
                tmpThirdEql.setSubstatement(expression.exp_statement.getSubstatement().get(0).getSubstatement().get(1).getSubstatement().get(1).getSubstatement());
                for (Statement tmp_statement : tmpThirdEql.getSubstatement()) {
                    //System.out.println("SELECT 479 DEBUG: tmp_statement: " + tmp_statement.getSubstatement().get(0).getContent());
                    if (tmp_statement.getSubstatement().get(0).getContent().equalsIgnoreCase("t")) {
                        tmp_statement.getSubstatement().get(0).setContent("rnaturalt.t");
                    }
                }

                Expression thirdexpression = new Expression(tmpThirdEql);
                ArrayList<String> fields = new ArrayList<String>();
                fields.add("*");
                ParserHelper.filter(schema_manager, memory, secondJoinR, thirdexpression, fields, 0);
                return null;
            }

            Relation relationAfterCross = MultiRelationCrossJoin(schema_manager, memory, relationList, 1);
            ArrayList<String> fields = new ArrayList<>();
            fields = relationAfterCross.getSchema().getFieldNames();
            if (expression != null) {

                if (!distinct && order == null) {
                    ParserHelper.filter(schema_manager, memory, relationAfterCross, expression, fields, 0);
                    return null;
                } else {

                    relationAfterCross = ParserHelper.filter(schema_manager, memory, relationAfterCross, expression, fields, 1);
                }
            }
            // if(expression == null && )
            if (distinct) {
                if (fields.get(0).equals("*")) {
                    //System.out.print( "SELECT 336 DEBUG: ");
                    fields = relationAfterCross.getSchema().getFieldNames();
                }
                if (order == null) {
                    ParserHelper.executeDistinct(schema_manager, memory, relationAfterCross, fields, 0);
                    return null;
                } else {
                    relationAfterCross = ParserHelper.executeDistinct(schema_manager, memory, relationAfterCross, fields, 1);

                }
            }

            if (order != null) {
                fields = new ArrayList<>();
                fields.add(order.getSubstatement().get(0).getSubstatement().get(0).getContent() + "." + order.getSubstatement().get(0).getSubstatement().get(1).getContent());
                ParserHelper.executeOrder(schema_manager, memory, relationAfterCross, fields, 0);
                return null;
            }

            if (expression == null && !fields.get(0).equals("*")) {
                int total = relationAfterCross.getNumOfBlocks();
                for (int i = 0; i < total; i++) {
                    relationAfterCross.getBlock(i, 0);
                    ArrayList<Tuple> tuples = memory.getBlock(0).getTuples();
                    for (Tuple tp : tuples) {
                        for (String f : fields) {
                            //System.out.print( "361 ");
                            System.out.print(tp.getField(f).toString() + "  ");
                        }
                        System.out.println();
                    }
                }
            }
        }
        return null;
    }

    public static Relation MultiRelationCrossJoin(SchemaManager schema_manager, MainMemory memory, ArrayList<String> relationName, int mode) {
        //cross join plan
        int memsize = memory.getMemorySize();
        if (relationName.size() == 2) {

            return ParserHelper.executeCrossJoin(schema_manager, memory, relationName, mode);
        } else {
            //run a DP algorithm to determine the order of join.
            HashMap<Set<String>, CrossRelation> singleRelation = new HashMap<>();
            for (String name : relationName) {
                HashSet<String> set = new HashSet<>();
                set.add(name);
                Relation relation = schema_manager.getRelation(name);
                CrossRelation temp = new CrossRelation(set, relation.getNumOfBlocks(), relation.getNumOfTuples());
                temp.cost = relation.getNumOfBlocks();
                temp.fieldNum = relation.getSchema().getNumOfFields();
                singleRelation.put(set, temp);
            }
            List<HashMap<Set<String>, CrossRelation>> costRelationList = new ArrayList<>();
            costRelationList.add(singleRelation);
            for (int i = 1; i < relationName.size(); i++) {
                costRelationList.add(new HashMap<Set<String>, CrossRelation>());
            }

            Set<String> finalGoal = new HashSet<>(relationName);
            CrossRelation cr = Algorithms.findOptimal(costRelationList, finalGoal, memsize);
            Algorithms.travesal(cr, 0);
            if (mode == 0) {
                helper(cr, memory, schema_manager, 0);
            } else {
                return helper(cr, memory, schema_manager, 1);
            }
            return null;
        }
    }

    public static Relation helper(CrossRelation cr, MainMemory memory, SchemaManager schema_manager, int mode) {
        if (cr.joinBy == null || cr.joinBy.size() < 2) {
            List<String> relation = new ArrayList<>(cr.subRelation);
            assert relation.size() == 1;
            return schema_manager.getRelation(relation.get(0));
        } else {
            assert cr.joinBy.size() == 2;
            if (mode == 0) {
                String subRelation1 = helper(cr.joinBy.get(0), memory, schema_manager, 1).getRelationName();
                String subRelation2 = helper(cr.joinBy.get(1), memory, schema_manager, 1).getRelationName();
                ArrayList<String> relationName = new ArrayList<>();
                relationName.add(subRelation1);
                relationName.add(subRelation2);
                return ParserHelper.executeCrossJoin(schema_manager, memory, relationName, 0);
            } else {
                String subRelation1 = helper(cr.joinBy.get(0), memory, schema_manager, 1).getRelationName();
                String subRelation2 = helper(cr.joinBy.get(1), memory, schema_manager, 1).getRelationName();
                ArrayList<String> relationName = new ArrayList<>();
                relationName.add(subRelation1);
                relationName.add(subRelation2);
                return ParserHelper.executeCrossJoin(schema_manager, memory, relationName, 1);
            }
        }
    }

    private void print(Tuple tuple, List<String> fieldList) {
        if (fieldList.get(0).equals("*")) {
            System.out.println(tuple);
            return;
        }

        for (String field : fieldList) {
            if (field.indexOf('.') > 0) { //table.attr
                String tmp_field = field.substring(field.indexOf('.') + 1);
                System.out.print((tuple.getSchema().getFieldType(tmp_field) == FieldType.INT ?
                        tuple.getField(tmp_field).integer : tuple.getField(tmp_field).str) + "   ");
            } else { //attr
                System.out.print((tuple.getSchema().getFieldType(field) == FieldType.INT ?
                        tuple.getField(field).integer : tuple.getField(field).str) + "   ");
            }
        }
        System.out.println();
    }

    private void printTitle(Tuple tuple, List<String> fieldList) {
        if (fieldList.get(0).equals("*")) {
            for (String fieldNames : tuple.getSchema().getFieldNames()) {
                System.out.print(fieldNames + "   ");
            }
            System.out.println();
        } else {
            for (String str : fieldList) {
                System.out.print(str + "    ");
            }
            System.out.println();
        }
    }

    public void superiorSelect(MainMemory memory, SchemaManager schema_manager, String relationName,
                               ArrayList<String> field, Expression exp, String orderBy, boolean distinct) {
        Relation relation = schema_manager.getRelation(relationName);

        if (exp != null) {
            Schema schema = relation.getSchema();
            Relation tempRelation = schema_manager.createRelation(relationName + "temp", schema);
            int tempRelationCurrentBlock = 0;
            Block tempBlock = memory.getBlock(1);
            tempBlock.clear();
            int count = 0;
            for (int i = 0; i < relation.getNumOfBlocks(); i++) {
                relation.getBlock(i, 0);
                ArrayList<Tuple> tuples = memory.getBlock(0).getTuples();
                for (Tuple tuple : tuples) {
                    if (exp.evaluateBoolean(tuple)) {
                        if (!tempBlock.isFull()) tempBlock.appendTuple(tuple);
                        else {
                            memory.setBlock(1, tempBlock);
                            tempRelation.setBlock(tempRelationCurrentBlock, 1);
                            tempRelationCurrentBlock += 1;
                            tempBlock.clear();
                            tempBlock.appendTuple(tuple);
                        }
                    }
                }
            }

            if (!tempBlock.isEmpty()) {
                memory.setBlock(1, tempBlock);
                tempRelation.setBlock(tempRelationCurrentBlock, 1);
                tempBlock.clear();
            }
            relation = tempRelation;
        }

        System.out.println("Number of tuples: " + relation.getNumOfTuples() + "*******");

        if (relation.getNumOfBlocks() <= memory.getMemorySize()) {
            relation.getBlocks(0, 0, relation.getNumOfBlocks());
            ArrayList<Tuple> tuples = memory.getTuples(0, relation.getNumOfBlocks());
            Algorithms.sortInMemory(tuples, orderBy);
            if (distinct) {
                Algorithms.removeDuplicate(tuples, field);
            }
            printTitle(tuples.get(0), field);
            for (Tuple tuple : tuples) {
                //System.out.println("619 ");
                print(tuple, field);
            }
        } else {
            System.out.println("Two pass condition");
            ArrayList<String> order = new ArrayList<>();
            if (orderBy != null) {
                order.add(orderBy);
            }
            if (field.get(0).equals("*")) {
                field = relation.getSchema().getFieldNames();
            }
            if (distinct && orderBy != null) {
                relation = ParserHelper.executeDistinct(schema_manager, memory, relation, field, 1);
                ParserHelper.executeOrder(schema_manager, memory, relation, order, 0);
            } else if (distinct) {
                ParserHelper.executeDistinct(schema_manager, memory, relation, field, 0);
            } else if (orderBy != null) {
                ParserHelper.executeOrder(schema_manager, memory, relation, order, 0);
            }
        }
    }

    private Relation regularSelect(MainMemory memory, SchemaManager schema_manager, String relationName, List<String> field, Expression exp) {
        int currentBlockCount = 0;
        Relation relation = schema_manager.getRelation(relationName);
        boolean show = false;
        while (currentBlockCount < relation.getNumOfBlocks()) {
            int readBlocks = relation.getNumOfBlocks() - currentBlockCount > memory.getMemorySize() ?
                    memory.getMemorySize() : relation.getNumOfBlocks() - currentBlockCount;
            relation.getBlocks(currentBlockCount, 0, readBlocks);
            ArrayList<Tuple> tuples = memory.getTuples(0, readBlocks);
            if (!show) {
                show = true;
                if (field.get(0).equals("*")) {
                    //System.out.print( "572 ");
                    for (String fieldNames : tuples.get(0).getSchema().getFieldNames()) {
                        System.out.print(fieldNames + "   ");
                    }
                    System.out.println();
                } else {
                    for (String name : field) System.out.print(name + "  ");
                    System.out.println();
                }
            }
            for (Tuple tuple : tuples) {
                if (exp == null) print(tuple, field);
                else {
                    if (exp.evaluateBoolean(tuple)) print(tuple, field);
                }
            }
            currentBlockCount += readBlocks;
        }
        return null;
    }

    public static void cycle(List<Statement> li, BufferedWriter bw) throws IOException {
        int pos = 0;
        while (pos < li.size()) {
            Statement field = li.get(pos);
            if (field.getSubstatement().size() == 1) { // attr
                System.out.print(field.getSubstatement().get(0).getContent() + "  ");
                bw.write(field.getSubstatement().get(0).getContent() + "  ");
            } else { // table.attr
                System.out.print(field.getSubstatement().get(0).getContent() + "." + field.getSubstatement().get(1).getContent() + "  ");
                bw.write(field.getSubstatement().get(0).getContent() + "." + field.getSubstatement().get(1).getContent() + "  ");
            }
        }
    }

    public static void output(Tuple t, Statement field, int idx, BufferedWriter bw)throws IOException{
        if (t.getSchema().getFieldType(field.getSubstatement().get(idx).getContent()) == FieldType.INT) {
            System.out.print(t.getField(field.getSubstatement().get(idx).getContent()).integer + "   ");

            bw.write(t.getField(field.getSubstatement().get(idx).getContent()).integer + "   ");


        } else {
            System.out.print(t.getField(field.getSubstatement().get(idx).getContent()).str + "   ");

            bw.write(t.getField(field.getSubstatement().get(idx).getContent()).str + "   ");
        }
    }

    public static void cycle1(Tuple t, List<Statement> li, BufferedWriter bw) throws IOException {
        int pos = 0;
        for (Statement field : li) {
            if (field.getSubstatement().size() == 1) {
                output(t, field, 0, bw);
            } else {
                output(t, field, 1, bw);
            }
        }

    }
}
