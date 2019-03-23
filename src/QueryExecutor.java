import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

import java.util.HashMap;
import java.util.List;

public class QueryExecutor {
    //members and constructor
    MainMemory memory;
    Disk disk;
    SchemaManager schema_manager;
    
    //A operation map help executor guide to certain key-word executor
    HashMap<String, Operation> executor_operation_map = new HashMap<String, Operation>();
    
    public QueryExecutor(MainMemory memory, Disk disk, SchemaManager schema_manager) {
        //initial StorageManager arguments
        this.memory = memory;
        this.disk = disk;
        this.schema_manager = schema_manager;
        
        //set up the operation map
        executor_operation_map.put("CREATE", new Create());
        executor_operation_map.put("INSERT", new Insert());
        executor_operation_map.put("SELECT", new Select());
        executor_operation_map.put("DELETE", new Delete());
        executor_operation_map.put("DROP", new Drop());
        
        //copy operation map to Operation class
        Operation.operation_map = executor_operation_map;
    }

    public void execute(Statement statement) {
        //record initial simulated time and dick I/O
        double simu_time = disk.getDiskTimer();
        long simu_IO = disk.getDiskIOs();
        
        //set up argument
        List<Statement> tmp_list = statement.getSubstatement();
        String tmp_state = statement.getContent();
        Argument argument = new Argument(tmp_list);
        argument.schema_manager = this.schema_manager;
        argument.memory = this.memory;
        argument.disk = this.disk;
        
        Operation operation = executor_operation_map.get(tmp_state);
        operation.execute(argument);
        
        //output simulated elapse time and disk IO
        System.out.printf("The processing time = %.2f ms\n"  ,(disk.getDiskTimer()-simu_time));
        System.out.println("The number of used Disk I/Os = " + (disk.getDiskIOs()-simu_IO));
        System.out.println("\n");
    }    
}
