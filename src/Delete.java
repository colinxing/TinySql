import storageManager.Relation;
import storageManager.Tuple;

import java.util.ArrayList;
import java.util.List;

// Delete part, redirect from operation

public class Delete extends Operation{
    public Argument execute(Argument argument) {
        List<Statement> del_args = argument.arguments;
        assert del_args != null : "ERROR: Not delete statement";

        Statement table = null;
        Statement condition = null;
        for (Statement item : del_args) {
            if (item.getContent().equalsIgnoreCase("TableName")) table = item;
            else if (item.getContent().equalsIgnoreCase("EXPRESSION")) condition = item;
        }

        assert table != null;
        if (condition == null) {
            Relation del_relation = argument.schema_manager.getRelation(table.getSubstatement().get(0).getContent());
            // if block has no content, delete directly
            del_relation.deleteBlocks(0);
        } else {
            // delete tuple
            delete(argument, table, condition);
        }
        return null;
    }

    public void delete(Argument argument, Statement table, Statement condition)
    {
        Relation del_relation = argument.schema_manager.getRelation(table.getSubstatement().get(0).getContent());
        Expression del_condition = new Expression(condition);

        int blk_num = del_relation.getNumOfBlocks();
        for (int i = 0; i < blk_num; i++) {
            boolean deleted = false;
            del_relation.getBlock(i, 0);
            ArrayList<Tuple> find_tuples = argument.memory.getBlock(0).getTuples();
            for (int j = 0; j < find_tuples.size(); j++) {
                if (del_condition.evaluateBoolean(find_tuples.get(j))) {
                    argument.memory.getBlock(0).invalidateTuple(j);
                    deleted = true;

                }
            }
            if (deleted) {
                del_relation.setBlock(i, 0);
                System.out.println("DELETE COMPLETE");
            }
        }
    }
}
