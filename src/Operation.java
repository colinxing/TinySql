import java.util.HashMap;


/*
 * class Operation-guider the executor to specific keyword executor
*/

public class Operation {
    //operation map, point to different file
    protected static HashMap<String, Operation> operation_map;
    
    public Argument execute(Argument argument) {
        //just a dummy for children 
        return null;
    }
}