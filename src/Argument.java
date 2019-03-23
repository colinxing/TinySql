import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

import java.io.FileWriter;
import java.util.List;

//Write to file

public class Argument {
    //variables
    public List<Statement> arguments;
    SchemaManager schema_manager;
    MainMemory memory;
    Disk disk;
    
    //methods
    public Argument(List<Statement> list) {
        arguments = list;
    }
    public FileWriter fw;
    
}