import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

// This is for tree statements
public class Statement {
    public String content; //content words or content
    public List<Statement> substatement; // define variable substatement which is List<Note> type
    HashSet<String> contains_table;
    
    public Statement(String content) {
        this.content = content;
        substatement = new ArrayList<Statement>();
    }
    
    //leaf
    public Statement(String content, boolean leaf) {
        this.content = content;
        substatement = null;//is leaf, substatement = null
    }
    
    public List<Statement> getSubstatement() {
        return substatement;
    }
    
    public String getContent() {
        return content;
    }
    
    public void setContent(String content) {
        this.content = content;
    }
    
    public void setSubstatement(List<Statement> substatement) {
        this.substatement = substatement;
    }
}
