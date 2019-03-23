import java.util.List;

// Drop table part, redirect from operation

public class Drop extends Operation {
    @Override
    public Argument execute(Argument argument) 
    {
        List<Statement> dropStatements = argument.arguments;

        if(dropStatements == null)
            throw new RuntimeException("Drop command error");

        argument.schema_manager.deleteRelation(dropStatements.get(0).getSubstatement().get(0).getContent());
        System.out.println("DROP: Successfully drop relation "+ dropStatements.get(0).getSubstatement().get(0).getContent());

        return null;
    }
}
