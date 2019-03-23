import storageManager.*;

import java.io.*;
import java.util.*;

public class ParserHelper {
  // An example procedure of appending a tuple to the end of a relation
  // using memory block "memory_block_index" as output buffer
  public static String sanitize(String str){
    String str_Travers = "";
    for(int i = 0; i < str.length(); i++)
    {
      if(str.charAt(i) != ',' && str.charAt(i) != ';' && str.charAt(i) != '(' && str.charAt(i) != ')')
        str_Travers += str.charAt(i);
    }
    str_Travers = str_Travers.replace("\"","");
    return str_Travers;
  }

  public static Boolean isEmpty(ArrayList<ArrayList<Tuple>> tuples){
    for(ArrayList<Tuple> tmp:tuples){
      if(tmp.size() == 0)
        continue;
      else
        return false;
    }
    return true;
  }

  public static Boolean isInteger(String s){
    for(int i = 0; i < s.length(); i++)
    {
      if(i==0 && s.charAt(i) == '-') continue;
      if(!Character.isDigit(s.charAt(i))) return false;
    }
    return true;
  }

  public static Boolean isEqual(Tuple t1, Tuple t2, ArrayList<String> fields){
    if(t1==null || t2==null) return false;

    for(String s:fields){
      String stringFormT1 = t1.getField(s).toString();
      String stringFormT2 = t2.getField(s).toString();

      if(!stringFormT1.equals(stringFormT2))
        return false;
    }
    return true;
  }

  public static  Tuple mergeTuple(Relation relation, Tuple tuple_1, Tuple tuple_2){
    Tuple t = relation.createTuple();
    int i = 0;

    while(i<tuple_1.getNumOfFields()+tuple_2.getNumOfFields()) {

      if(i<tuple_1.getNumOfFields()){
        String str = tuple_1.getField(i).toString();
        if(isInteger(str))
          t.setField(i,Integer.parseInt(str));
        else
          t.setField(i,str);
      }else{
        String str = tuple_2.getField(i-tuple_1.getNumOfFields()).toString();
        if(isInteger(str))
          t.setField(i,Integer.parseInt(str));
        else
          t.setField(i,str);
      }

      i++;
    }
    return t;
  }

  private static Relation getRelation(MainMemory mem, ArrayList<Tuple> output, Schema schema, Relation new_relation) {
    int blk_cnt = 0;
    Block blk = mem.getBlock(0);
    while(!output.isEmpty()){
      blk.clear();
      for(int i=0;i<schema.getTuplesPerBlock();i++){
        if(!output.isEmpty()){
          Tuple t = output.get(0);
          blk.setTuple(i,t);
          output.remove(t);
        }
      }
      new_relation.setBlock(blk_cnt++,0);
    }
    return new_relation;
  }

  static void mergeNameHelper(ArrayList<String> fieldNames, ArrayList<String> newField, String Name)
  {
    for(String str : fieldNames){
      if(!str.contains("\\.")){
        StringBuffer sb = new StringBuffer(str);
        sb.insert(0,Name+".");
        newField.add(sb.toString());
      }else{
        newField.add(str);
      }
    }
    return;
  }

  static void mergeTypeHelper(ArrayList<FieldType> fieldType, ArrayList<FieldType> newField)
  {
    for(FieldType ft : fieldType) {
      newField.add(ft);
    }
    return;
  }

  public  static  Schema mergeSchema(SchemaManager schema_manager,String r_name,String s_name){
    ArrayList<String> rFieldNames = schema_manager.getRelation(r_name).getSchema().getFieldNames();
    ArrayList<String> sFieldNames = schema_manager.getRelation(s_name).getSchema().getFieldNames();

    ArrayList<FieldType> rFieldType = schema_manager.getRelation(r_name).getSchema().getFieldTypes();
    ArrayList<FieldType> sFieldType = schema_manager.getRelation(s_name).getSchema().getFieldTypes();
    ArrayList<String> fieldNamesToReturn = new ArrayList<String>();
    ArrayList<FieldType> fieldTypesToReturn = new ArrayList<FieldType>();

    mergeNameHelper(rFieldNames, fieldNamesToReturn,r_name);
    mergeNameHelper(sFieldNames, fieldNamesToReturn,s_name);

    mergeTypeHelper(rFieldType,fieldTypesToReturn);
    mergeTypeHelper(sFieldType,fieldTypesToReturn);

    Schema schema = new Schema(fieldNamesToReturn,fieldTypesToReturn);
    return schema;
  }

  public static int getMinCount(ArrayList<ArrayList<Tuple>> tuples,String field, String minValue){
    int res=0;
    for(ArrayList<Tuple> iterator:tuples){
      for(Tuple t:iterator){
        if(t.getField(field).toString().equals(minValue))
          res+=1;
      }
    }
    return res;
  }

  public static int getBlockMinCount(ArrayList<Tuple> tuples,String field, String minValue){
    int res=0;
    for(Tuple t:tuples){
        if(t.getField(field).toString().equals(minValue))
          res+=1;
    }
    return res;
  }

  public static ArrayList<Tuple> getMinTuples(ArrayList<ArrayList<Tuple>> tuples,String field, String minValue){
    ArrayList<Tuple> res = new ArrayList<Tuple>();
    for(ArrayList<Tuple> iter:tuples){
      for(Tuple t:iter){
        if(t.getField(field).toString().equals(minValue))
          res.add(t);
      }
    }
    return res;
  }

  public static ArrayList<ArrayList<Tuple>> deleteMin(ArrayList<ArrayList<Tuple>> tuples,String field, String value){
    int i=0;
    while(i<tuples.size()){
      int j=0;
      while(j<tuples.get(i).size()){
        Tuple t = tuples.get(i).get(j);
        if(t.getField(field).toString().equals(value)){
          tuples.get(i).remove(t);
        }
        j+=1;
      }
      i+=1;
    }
    return tuples;
  }

  public static void appendTuple(Relation relation_reference, MainMemory mem, int blockIndex, Tuple tuple) {
    Block block;
    if (relation_reference.getNumOfBlocks()==0) {
      block=mem.getBlock(blockIndex);

      block.clear(); //clear the block
      block.appendTuple(tuple); // append the tuple

      relation_reference.setBlock(relation_reference.getNumOfBlocks(),blockIndex);
    } else {
      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,blockIndex);
      block=mem.getBlock(blockIndex);
      if (block.isFull()) {

        block.clear(); //clear the block
        block.appendTuple(tuple); // append the tuple

        relation_reference.setBlock(relation_reference.getNumOfBlocks(),blockIndex); //write back to the relation
      } else {

//        block.clear(); //clear the block
        block.appendTuple(tuple); // append the tuple

        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,blockIndex); //write back to the relation
      }
    }
  }

  public static int com(Tuple t1, Tuple t2, final ArrayList<String> indexField)
  {
    int comparator;
    for(int i=0;i<indexField.size();i++){
      String str1 = t1.getField(indexField.get(i)).toString();
      String str2 = t2.getField(indexField.get(i)).toString();
      //set result by measure str1 and str2
      comparator = (isInteger(str1) && isInteger(str2))? Integer.parseInt(str1)-Integer.parseInt(str2) : str1.compareTo(str2);
      if(comparator>0)
        return 1;
      if(comparator<0)
        return -1;
    }
    //If equal
    return 0;
  }

  public static int twoPassPhaseOne(Relation relation, MainMemory mem, final ArrayList<String> indexField){

    int readToReturn=0;
    int sortedBlocks = 0;

    while(sortedBlocks<relation.getNumOfBlocks()){
      readToReturn = ((relation.getNumOfBlocks()-sortedBlocks)>mem.getMemorySize())?mem.getMemorySize():(relation.getNumOfBlocks()-sortedBlocks);
      relation.getBlocks(sortedBlocks,0,readToReturn);
      ArrayList<Tuple> tuples = mem.getTuples(0,readToReturn);

//      MyComparator comparator= new ComparatorUser();
//      Collections.sort(tuples, comparator);
//      refine comparator for the sort function

      Collections.sort(tuples,new Comparator<Tuple>(){
          public int compare(Tuple t1, Tuple t2){
            if(t1==null) return 1;
            if(t2==null) return -1;
            return com(t1, t2, indexField);
          }
      });
      mem.setTuples(0,tuples);

      relation.setBlocks(sortedBlocks,0,readToReturn);
      sortedBlocks+=readToReturn;
    }
    return readToReturn;
  }

  public static Relation executeCrossJoin(SchemaManager schema_manager, MainMemory mem, ArrayList<String> relationName, int mode){
    ArrayList<Tuple> output;
    Relation relation;
    if(relationName.size()==2){

      String r1 = relationName.get(0);
      String r2 = relationName.get(1);
      System.out.println("SELECT: cross join from two relations\n");
      Relation r = schema_manager.getRelation(relationName.get(0));
      Relation s = schema_manager.getRelation(relationName.get(1));
      // System.out.println("SELECT: print block number of relation "+relationName.get(0)+" : "+r.getNumOfBlocks());
      // System.out.println("SELECT: print block number of relation "+relationName.get(1)+" : "+s.getNumOfBlocks()+"\n");

      Relation smaller = (r.getNumOfBlocks()<=s.getNumOfBlocks())?r:s;

      if(smaller.getNumOfBlocks()<mem.getMemorySize()-1){
        System.out.println("SELECT: cross join smaller relation can fit memory\n");
        output = onePassJoin(schema_manager,mem,relationName.get(0),relationName.get(1));
      }else{
        System.out.println("SELECT: cross join smaller relation cannot fit memory\n");
        output = nestedJoin(schema_manager,mem,relationName.get(0),relationName.get(1));
      }
      if(mode==0){
        System.out.println(output.get(0).getSchema().fieldNamesToString());
        for(Tuple t:output){
          System.out.println(t);
        }
        return null;
      }else{
        Schema schema = mergeSchema(schema_manager,relationName.get(0),relationName.get(1));

        if(!schema_manager.relationExists(r1+"cross"+r2)){
          relation = schema_manager.createRelation(r1+"cross"+r2,schema);
        }
        else{
          schema_manager.deleteRelation(r1+"cross"+r2);
          relation = schema_manager.createRelation(r1+"cross"+r2,schema);
        }
        return getRelation(mem, output, schema, relation);
      }
    }else {
      // System.out.println("SELECT: Multiple relations join\n");
      return null;
    }
  }

  public static Relation executeNaturalJoin(SchemaManager schema_manager, MainMemory mem, String r1, String r2, String field, int mode){
    ArrayList<Tuple> output;
    Relation relation;

    System.out.println("SELECT: natural join from two relations\n");
    Relation r = schema_manager.getRelation(r1);
    Relation s = schema_manager.getRelation(r2);
    // System.out.println("SELECT: print block number of relation "+r1+" : "+r.getNumOfBlocks());
    // System.out.println("SELECT: print block number of relation "+r2+" : "+s.getNumOfBlocks()+"\n");

    Relation smaller = (r.getNumOfBlocks()<=s.getNumOfBlocks())?r:s;

    if(smaller.getNumOfBlocks() < mem.getMemorySize()-1) {
      System.out.println("SELECT: natural join smaller relation can fit memory\n");
      output = onePassNaturalJoin(schema_manager,mem,r1,r2,field);
    }else{
      System.out.println("SELECT: natural join smaller relation cannot fit memory\n");
      //System.out.println("ParserParserHelper 322 DEBUG: field: " + field + " r1: " + r1 + " r2: " + r2);
      output = twoPassNaturalJoin(schema_manager,mem,r1,r2,field);
    }
    if(mode==0) {
      System.out.println(output.get(0).getSchema().fieldNamesToString());
      for (Tuple t : output) {
        System.out.println(t);
      }
      return null;
    }else{
      Schema schema = mergeSchema(schema_manager,r1,r2);
      if(!schema_manager.relationExists(r1+"natural"+r2)){
        relation = schema_manager.createRelation(r1+"natural"+r2,schema);
      }
      else{
        schema_manager.deleteRelation(r1+"natural"+r2);
        relation = schema_manager.createRelation(r1+"natural"+r2,schema);
      }
        /*Need to be optimized to get block I/O*/
      return getRelation(mem, output, schema, relation);
    }
  }

  private static Schema getNewSchema(Relation inputRelation, ArrayList<String> fields) {
    ArrayList<FieldType> fieldTypes = new ArrayList<>();

    if(fields.size()==1 && fields.get(0).equals("*")){
      fields.remove(0);
      for(int i=0;i<inputRelation.getSchema().getNumOfFields();i++)
        fields.add(inputRelation.getSchema().getFieldName(i));
    }

    for(String s:fields){
      fieldTypes.add(inputRelation.getSchema().getFieldType(s));
    }

    return new Schema(fields,fieldTypes);
  }

  public static Relation filter(SchemaManager schema_manager, MainMemory mem, Relation inputRelation, Expression expression, ArrayList<String> fields, int mode){
    int i = 0;
    List<Tuple> filteredRelation = new ArrayList<>();
    while(i<inputRelation.getNumOfBlocks()){
      Block block = mem.getBlock(0);
      inputRelation.getBlock(i,0);

      for(Tuple tmp:block.getTuples()){
        if(expression.evaluateBoolean(tmp))
          filteredRelation.add(tmp);
      }
      i++;
    }
    
    Schema new_schema = getNewSchema(inputRelation, fields);
    ArrayList<Tuple> output = new ArrayList<>();
    if(schema_manager.relationExists(inputRelation.getRelationName()+"filtered"))
      schema_manager.deleteRelation(inputRelation.getRelationName()+"filtered");

    Relation relation = schema_manager.createRelation(inputRelation.getRelationName()+"filtered",new_schema);

    for(Tuple t:filteredRelation){
      Tuple tuple = relation.createTuple();
      for(String s:new_schema.getFieldNames()){
        if(t.getField(s).type==FieldType.INT)
          tuple.setField(s,Integer.parseInt(t.getField(s).toString()));
        else
          tuple.setField(s,t.getField(s).toString());
      }
      output.add(tuple);
    }

    if(mode==0){
      System.out.println(new_schema.fieldNamesToString());
      for(Tuple t:output){
        System.out.println(t);
      }
      return null;
    }else {
      return getRelation(mem, output, new_schema, relation);
    }
  }

  public static Relation executeOrder(SchemaManager schema_manager, MainMemory mem, Relation relation, ArrayList<String> fieldList,int mode){
    ArrayList<Tuple> output = new ArrayList<Tuple>();
    if(relation.getNumOfBlocks()<mem.getMemorySize()) {
      //one pass for sort of 1 relation
      System.out.println("SELECT: One pass for sorting on 1 relation\n");
      output = onePassSort(relation, mem, fieldList);
    }else{
      System.out.println("SELECT: Two pass for sorting on 1 relation\n");
      output = twoPassSort(relation,mem,fieldList);
    }
    if(mode==0){
      System.out.println(relation.getSchema().fieldNamesToString());
      for(Tuple t:output)
        System.out.println(t);
      return null;
    }else{
      Schema schema = output.get(0).getSchema();
      if(schema_manager.relationExists(relation.getRelationName()+"ordered"))
        schema_manager.deleteRelation(relation.getRelationName()+"ordered");
      Relation new_relation = schema_manager.createRelation(relation.getRelationName()+"ordered",schema);
      return getRelation(mem, output, schema, new_relation);
    }
  }

  public static Relation executeDistinct(SchemaManager schema_manager, MainMemory mem, Relation relation, ArrayList<String> fieldList,int mode) {
    ArrayList<Tuple> output = new ArrayList<Tuple>();
    if (relation.getNumOfBlocks() < mem.getMemorySize()) {
      //System.out.println("ParserHelper 488 DEBUG: ");
      output = onePassDistinct(relation, mem, fieldList);
      //System.out.println("ParserHelper 490 DEBUG: ");
    } else {
      output = twoPassDistinct(relation, mem, fieldList);
    }
//    return outputMethod(schema_manager, mem, mode, output);
    if (mode == 0) {
      System.out.println(relation.getSchema().fieldNamesToString());
      for (Tuple t : output)
        System.out.println(t);
      return null;
    } else {
      Schema schema = output.get(0).getSchema();
      if(schema_manager.relationExists(relation.getRelationName()+"distinct"))
        schema_manager.deleteRelation(relation.getRelationName()+"distinct");
      Relation new_relation = schema_manager.createRelation(relation.getRelationName() + "distinct", schema);
      return getRelation(mem, output, schema, new_relation);
    }
  }


  public static ArrayList<Tuple> onePassSort(Relation relation, MainMemory mem, final ArrayList<String> indexField){
    relation.getBlocks(0,0,relation.getNumOfBlocks());
    ArrayList<Tuple> tuples = mem.getTuples(0,relation.getNumOfBlocks());
    ArrayList<Tuple> output=new ArrayList<Tuple>();

    Collections.sort(tuples,new Comparator<Tuple>(){
        public int compare(Tuple t1, Tuple t2){
          if(t1==null) return 1;
          if(t2==null) return -1;
          return com(t1, t2, indexField);
        }
    });
    for(Tuple t : tuples) 
      output.add(t);
    return output;
  }

  public static ArrayList<Tuple> twoPassSort(Relation relation, MainMemory mem, final ArrayList<String> indexField){

    
    int temp=0,printed=0,index=0;
    ArrayList<Integer> segments=new ArrayList<Integer>();
    ArrayList<Tuple> output=new ArrayList<Tuple>();
    int last_segment = twoPassPhaseOne(relation,mem,indexField);
    while(temp<relation.getNumOfBlocks()){
      segments.add(temp);
      temp+=mem.getMemorySize();
    }
    Block block = null;
    for(int i=0;i<mem.getMemorySize();i++){
      block = mem.getBlock(i); //access to memory block 0
      block.clear(); //clear the block
    }
    int[] reads = new int[segments.size()];
    Arrays.fill(reads,1);
    ArrayList<ArrayList<Tuple>> tuples = new ArrayList<ArrayList<Tuple>>();
    for(int i=0;i<segments.size();i++){
      relation.getBlock(segments.get(i),i);
      block = mem.getBlock(i);
      tuples.add(block.getTuples());
    }
    Tuple[] minTuple = new Tuple[segments.size()];

    for(int i=0;i<relation.getNumOfTuples();i++){
      for(int j=0;j<segments.size();j++){
        if(tuples.get(j).isEmpty()){
          if((j<segments.size()-1 && reads[j]<mem.getMemorySize()) || (j==segments.size()-1 && reads[j]<last_segment)){
            relation.getBlock(segments.get(j)+reads[j],j);
            block = mem.getBlock(j);
            tuples.get(j).addAll(block.getTuples());
            reads[j]++;
          }
        }
      }
      for(int k=0;k<segments.size();k++){
        if(!tuples.get(k).isEmpty()){
           minTuple[k] = Collections.min(tuples.get(k),new Comparator<Tuple>(){
            public int compare(Tuple t1, Tuple t2){
              if(t1==null) return 1;
              if(t2==null) return -1;
              return com(t1, t2, indexField);
            }
          });
        }else{
          minTuple[k] = null;
        }
      }
      ArrayList<Tuple> tmp = new ArrayList<Tuple>(Arrays.asList(minTuple));
      Tuple minVal = Collections.min(tmp,new Comparator<Tuple>(){
            public int compare(Tuple t1, Tuple t2){
              if(t1==null) return 1;
              if(t2==null) return -1;
              return com(t1, t2, indexField);
            }
          });

      int resultIndex = tmp.indexOf(minVal);
      int tupleIndex = tuples.get(resultIndex).indexOf(minTuple[resultIndex]);

      tuples.get(resultIndex).remove(tupleIndex);
      output.add(minVal);
      printed++;
    }
    System.out.println("SELECT: Two Pass Sort: "+printed+" tuples printed\n");
    return output;
  }

  public static ArrayList<Tuple> onePassDistinct(Relation relation, MainMemory mem, final ArrayList<String> indexField){
    relation.getBlocks(0,0,relation.getNumOfBlocks());
    ArrayList<Tuple> tuples = mem.getTuples(0,relation.getNumOfBlocks());
    ArrayList<Tuple> output = new ArrayList<Tuple>();
    Tuple tmp = null;
    //for (Tuple tuple : tuples) {
    int tuples_size = tuples.size();
    Tuple tuple = null;
    while (tuples.size() != 0) {
      tuple = Collections.min(tuples,new Comparator<Tuple>(){
        public int compare(Tuple t1, Tuple t2){
          if(t1==null) return 1;
          if(t2==null) return -1;
          return com(t1, t2, indexField);
        }
      });
      if(!isEqual(tuple,tmp,indexField)){
        tmp = tuple;
        output.add(tuple);
      }
      tuples.remove(tuples.indexOf(tuple));
    }
    return output;
  }

  public static ArrayList<Tuple> twoPassDistinct(Relation relation, MainMemory mem, final ArrayList<String> indexField){
    int temp=0,printed=0;
    ArrayList<Integer> segments=new ArrayList<Integer>();
    ArrayList<Tuple> output=new ArrayList<Tuple>();
    
    // Phase 1
    int last_segment = twoPassPhaseOne(relation,mem,indexField);
    // Phase 2
    while(temp<relation.getNumOfBlocks()){
      segments.add(temp);
      temp+=mem.getMemorySize();
    }
    // System.out.println("SELECT: DISTINCT by two pass: "+segments.size()+" segments\n");
    Block block= null;
    for(int i=0;i<mem.getMemorySize();i++){
      block = mem.getBlock(i); //access to memory block 0
      block.clear(); //clear the block
      mem.setBlock(i,block);
    }
    int[] reads = new int[segments.size()];
    Arrays.fill(reads,1);

    // Initialize memory with first blocks
    ArrayList<ArrayList<Tuple>> tuples = new ArrayList<ArrayList<Tuple>>();
    for(int i=0;i<segments.size();i++){
      relation.getBlock(segments.get(i),i);
      block = mem.getBlock(i);
      tuples.add(block.getTuples());
    }

    Tuple comparator = null;
    for(int i=0;i<relation.getNumOfTuples();i++){
      //  Test if the block is empty, if is, read in next block in the segment
      for(int j=0;j<segments.size();j++){
        if(tuples.get(j).isEmpty()){
          if(j<segments.size()-1 && reads[j]<mem.getMemorySize()){
            relation.getBlock(segments.get(j)+reads[j],j);
            block = mem.getBlock(j);
            tuples.get(j).addAll(block.getTuples());
            reads[j]++;
          }else if(j==segments.size()-1 && reads[j]<last_segment){
            relation.getBlock(segments.get(j)+reads[j],j);
            block = mem.getBlock(j);
            tuples.get(j).addAll(block.getTuples());
            reads[j]++;
          }
        }
      }
      Tuple[] minTuple = new Tuple[segments.size()];
      for(int k=0;k<segments.size();k++){
        if(!tuples.get(k).isEmpty()){
           minTuple[k] = Collections.min(tuples.get(k),new Comparator<Tuple>(){
            public int compare(Tuple t1, Tuple t2){
              if(t1==null) return 1;
              if(t2==null) return -1;
              return com(t1, t2, indexField);
            }
          });
        }else{
          minTuple[k] = null;
        }
      }
      ArrayList<Tuple> tmp = new ArrayList<Tuple>(Arrays.asList(minTuple));
      
      Tuple minVal = Collections.min(tmp,new Comparator<Tuple>(){
          public int compare(Tuple t1, Tuple t2){
            if(t1==null) return 1;
            if(t2==null) return -1;
            return com(t1, t2, indexField);
            }
      });
      int resultIndex = tmp.indexOf(minVal);
      int tupleIndex = tuples.get(resultIndex).indexOf(minTuple[resultIndex]);
      if(!isEqual(minVal,comparator,indexField)){
        output.add(minVal);
        comparator = minVal;
        printed++;
      }
      tuples.get(resultIndex).remove(tupleIndex);
    }
    System.out.println("SELECT: Two Pass Duplicate Elimination: "+printed+" tuples printed\n");
    // System.out.print("Now the memory contains: " + "\n");
    // System.out.print(mem + "\n");
    return output;
  }

  public static ArrayList<Tuple> onePassJoin(SchemaManager schema_manager, MainMemory mem, String rName, String sName){
    ArrayList<Tuple> output = new ArrayList<Tuple>();

    Relation r = schema_manager.getRelation(rName);
    Relation s = schema_manager.getRelation(sName);

    Relation smaller = (r.getNumOfBlocks()<=s.getNumOfBlocks())?r:s;
    Relation larger = (smaller==r)?s:r;
    smaller.getBlocks(0,0,smaller.getNumOfBlocks());

    Schema schema = mergeSchema(schema_manager,rName,sName);

    if(schema_manager.relationExists(rName+"cross"+sName+"tmp"))
      schema_manager.deleteRelation(rName+"cross"+sName+"tmp");
    Relation relation = schema_manager.createRelation(rName+"cross"+sName+"tmp",schema);

    for(int j=0;j<larger.getNumOfBlocks();j++){
      larger.getBlock(j,mem.getMemorySize()-1);
      Block large_block = mem.getBlock(mem.getMemorySize()-1);
      for(Tuple tuple1 : mem.getTuples(0,smaller.getNumOfBlocks())){
        for(Tuple tuple2 : large_block.getTuples()){
          if(smaller==r){
            output.add(mergeTuple(relation,tuple1,tuple2));
          }else{
            output.add(mergeTuple(relation,tuple2,tuple1));
          }
        }
      }
    }
    return output;
  }

  public static ArrayList<Tuple> nestedJoin(SchemaManager schema_manager, MainMemory mem,String rName, String sName){
    ArrayList<Tuple> output = new ArrayList<Tuple>();

    //debuging
    //System.out.println("HELPER 861 DEBUG: r1: " + rName + " r2: " + sName);
    
    Relation r = schema_manager.getRelation(rName);
    Relation s = schema_manager.getRelation(sName);

    Schema schema = mergeSchema(schema_manager,rName,sName);
    
    //System.out.println("HELPER 868 DEBUG: schema fieldname: " + schema.fieldNamesToString());
    
    if(schema_manager.relationExists(rName+"cross"+sName+"tmp"))
      schema_manager.deleteRelation(rName+"cross"+sName+"tmp");
    Relation relation = schema_manager.createRelation(rName+"cross"+sName+"tmp",schema);

    for(int i=0;i<r.getNumOfBlocks();i++){
      r.getBlock(i,0);
      Block rblock = mem.getBlock(0);
      for(int j=0;j<s.getNumOfBlocks();j++){
        s.getBlock(j,1);
        Block sblock = mem.getBlock(1);
        for(Tuple tuple1 : rblock.getTuples()){
          for(Tuple tuple2 : sblock.getTuples()){
            output.add(mergeTuple(relation,tuple1,tuple2));
          }
        }
      }
    }
    return output;
  }

  public static ArrayList<Tuple> onePassNaturalJoin(SchemaManager schema_manager, MainMemory mem, String r1, String r2, String field){
    ArrayList<Tuple> output = new ArrayList<Tuple>();

    Relation r = schema_manager.getRelation(r1);
    Relation s = schema_manager.getRelation(r2);
    Relation smaller = (r.getNumOfBlocks()<=s.getNumOfBlocks())?r:s;
    Relation larger = (smaller==r)?s:r;
    smaller.getBlocks(0,0,smaller.getNumOfBlocks());

    Schema schema = mergeSchema(schema_manager,r1,r2);

    if(schema_manager.relationExists(r1+"natural"+r2+"tmp"))
      schema_manager.deleteRelation(r1+"natural"+r2+"tmp");

    Relation relation = schema_manager.createRelation(r1+"natural"+r2+"tmp",schema);

    for(int i=0;i<larger.getNumOfBlocks();i++){
      larger.getBlock(i,mem.getMemorySize()-1);
      Block block2 = mem.getBlock(mem.getMemorySize()-1);
      for(Tuple t2:block2.getTuples()){
        for(int j=0;j< smaller.getNumOfBlocks();j++){
          Block block1 = mem.getBlock(j);
          for(Tuple t1:block1.getTuples()){
            String s1 = t1.getField(field).toString();
            String s2 = t2.getField(field).toString();
            if(isInteger(s1)&&isInteger(s2)){
              if(Integer.parseInt(s1)==Integer.parseInt(s2)){
                if(smaller==r) output.add(mergeTuple(relation,t1,t2));
                else output.add(mergeTuple(relation,t2,t1));
              }
            }else{
              if(s1.equals(s2)){
                if(Integer.parseInt(s1)==Integer.parseInt(s2)){
                  if(smaller==r) output.add(mergeTuple(relation,t1,t2));
                  else output.add(mergeTuple(relation,t2,t1));
                }
              }
            }
          }
        }
      }
    }
    return output;
  }

  public static ArrayList<Tuple> twoPassNaturalJoin(SchemaManager schema_manager, MainMemory mem, String r1, String r2, final String field){
    int temp=0,printed=0,index=0;
    ArrayList<Integer> segments1=new ArrayList<Integer>();
    ArrayList<Integer> segments2=new ArrayList<Integer>();

    ArrayList<Tuple> output = new ArrayList<Tuple>();
    ArrayList<String> fieldList = new ArrayList<String>(); //always only have one element
    Relation r = schema_manager.getRelation(r1);
    Relation s = schema_manager.getRelation(r2);

    Schema schema = mergeSchema(schema_manager,r1,r2);
    //System.out.println("HELPER 941 DEBUG: schema field name: " + schema.fieldNamesToString());
    if(schema_manager.relationExists(r1+"natural"+r2+"tmp"))
      schema_manager.deleteRelation(r1+"natural"+r2+"tmp");

    Relation relation = schema_manager.createRelation(r1+"natural"+r2+"tmp",schema);

    //fieldList.add(field);
    
    //System.out.println("HELPER 958 DEBUG: r field name: " + r.getSchema().fieldNamesToString());
    //System.out.println("HELPER 959 DEBUG: s field name: " + s.getSchema().fieldNamesToString());
    
    String[] rFields = r.getSchema().fieldNamesToString().split("\t");
    for (String tmp_field : rFields) {
      if (tmp_field.charAt(tmp_field.length()-1) == field.charAt(field.length()-1)) {
        fieldList.add(tmp_field);
      }
    }

    int last_segment1 = twoPassPhaseOne(r,mem,fieldList);
    final String rField = fieldList.get(0); //for phase two use
    fieldList.clear();
    
    String[] sFields = s.getSchema().fieldNamesToString().split("\t");
    for (String tmp_field : sFields) {
      if (tmp_field.charAt(tmp_field.length()-1) == field.charAt(field.length()-1)) {
        fieldList.add(tmp_field);
      }
    }

    int last_segment2 = twoPassPhaseOne(s,mem,fieldList);
    final String sField = fieldList.get(0); //for phase two use
    fieldList.clear();
    //System.out.println("Help 983 DEBUG: after twoPassPhaseOne");
    while(temp<r.getNumOfBlocks()){
      segments1.add(temp);
      temp+=mem.getMemorySize();
    }

    temp = 0;
    while(temp<s.getNumOfBlocks()){
      segments2.add(temp);
      temp+=mem.getMemorySize();
    }
    //System.out.println("SELECT: TWO PASS NATURAL JOIN: segments1 size is "+segments1.size()+"\n");
    //System.out.println("SELECT: TWO PASS NATURAL JOIN: segments2 size is "+segments2.size()+"\n");
    Block block = null;
    for(int i=0;i<mem.getMemorySize();i++){
      block = mem.getBlock(i); //access to memory block 0
      block.clear(); //clear the block
    }
    int[] reads1 = new int[segments1.size()];
    int[] reads2 = new int[segments2.size()];

    Arrays.fill(reads1,1);
    Arrays.fill(reads2,1);

    ArrayList<ArrayList<Tuple>> tuples1 = new ArrayList<ArrayList<Tuple>>();
    ArrayList<ArrayList<Tuple>> tuples2 = new ArrayList<ArrayList<Tuple>>();

    for(int i=0;i<segments1.size();i++){
      r.getBlock(segments1.get(i),i);
      block = mem.getBlock(i);
      tuples1.add(block.getTuples());
    }

    for(int i=0;i<segments2.size();i++){
      s.getBlock(segments2.get(i),i+segments1.size());
      block = mem.getBlock(i+segments1.size());
      tuples2.add(block.getTuples());
    }

    Tuple[] minTuple1 = new Tuple[segments1.size()];
    Tuple[] minTuple2 = new Tuple[segments2.size()];

    while(!isEmpty(tuples1)&&!isEmpty(tuples2)) {

      for (int j = 0; j < segments1.size(); j++) {
        if (tuples1.get(j).isEmpty()) {
        //  System.out.println("SELECT: tuples1["+j+"] is empty\n");
          if ((j < segments1.size() - 1 && reads1[j] < mem.getMemorySize()) || (j == segments1.size() - 1 && reads1[j] < last_segment1)) {
            r.getBlock(segments1.get(j) + reads1[j], j);
            block = mem.getBlock(j);
            tuples1.get(j).addAll(block.getTuples());
            reads1[j]++;
          }
        }
      }

      for (int j = 0; j < segments2.size(); j++) {
        if (tuples2.get(j).isEmpty()) {
          if ((j < segments2.size() - 1 && reads2[j] < mem.getMemorySize()) || (j == segments2.size() - 1 && reads2[j] < last_segment2)) {
            s.getBlock(segments2.get(j) + reads2[j], j+segments1.size());
            block = mem.getBlock(j+segments1.size());
            tuples2.get(j).addAll(block.getTuples());
            reads2[j]++;
          }
        }
      }

      for (int k = 0; k < segments1.size(); k++) {
        if (!tuples1.get(k).isEmpty()) {
          minTuple1[k] = Collections.min(tuples1.get(k), new Comparator<Tuple>() {
            public int compare(Tuple t1, Tuple t2) {
              if (t1 == null) return 1;
              if (t2 == null) return -1;
              String s1 = t1.getField(rField).toString();
              String s2 = t2.getField(rField).toString();
              return (isInteger(s1) && isInteger(s2))? (Integer.parseInt(s1) - Integer.parseInt(s2)):s1.compareTo(s2);
            }
          });
        } else {
          minTuple1[k] = null;
        }
      }
      for (int k = 0; k < segments2.size(); k++) {
        if (!tuples2.get(k).isEmpty()) {
          minTuple2[k] = Collections.min(tuples2.get(k), new Comparator<Tuple>() {
            public int compare(Tuple t1, Tuple t2) {
              if (t1 == null) return 1;
              if (t2 == null) return -1;
              String s1 = t1.getField(sField).toString();
              String s2 = t2.getField(sField).toString();
              return (isInteger(s1) && isInteger(s2))? (Integer.parseInt(s1) - Integer.parseInt(s2)):s1.compareTo(s2);
            }
          });
        } else {
          minTuple2[k] = null;
        }
      }

      ArrayList<Tuple> tmp1 = new ArrayList<>(Arrays.asList(minTuple1));
      ArrayList<Tuple> tmp2 = new ArrayList<>(Arrays.asList(minTuple2));


      Tuple minVal1 = Collections.min(tmp1,new Comparator<Tuple>(){
        public int compare(Tuple t1, Tuple t2){
          if(t1==null) return 1;
          if(t2==null) return -1;
          String s1 = t1.getField(rField).toString();
          String s2 = t2.getField(rField).toString();
          return (isInteger(s1) && isInteger(s2))? (Integer.parseInt(s1) - Integer.parseInt(s2)):s1.compareTo(s2);
        }
      });

      Tuple minVal2 = Collections.min(tmp2,new Comparator<Tuple>(){
        public int compare(Tuple t1, Tuple t2){
          if(t1==null) return 1;
          if(t2==null) return -1;
          String s1 = t1.getField(sField).toString();
          String s2 = t2.getField(sField).toString();
          return (isInteger(s1) && isInteger(s2))? (Integer.parseInt(s1) - Integer.parseInt(s2)):s1.compareTo(s2);
        }
      });

      String min1=null,min2=null;
      if(minVal1!=null){
         min1 = minVal1.getField(rField).toString();
      }

      if(minVal2!=null){
        min2 = minVal2.getField(sField).toString();
      }

      if(min1!=null&&min2!=null&&min1.equals(min2)){
        /* Get all the minimum tuples in both collections, cross join and output */
        int count1 = getMinCount(tuples1,rField,min1);
        int count2 = getMinCount(tuples2,sField,min2);

        ArrayList<Tuple> minTuples1 = getMinTuples(tuples1,rField,min1);
        ArrayList<Tuple> minTuples2 = getMinTuples(tuples2,sField,min2);

        for(int i=0;i<count1;i++){
          for(int j=0;j<count2;j++)
            output.add(mergeTuple(relation,minTuples1.get(i),minTuples2.get(j)));
        }

        Boolean flag1 = false, flag2 = false;
        for(int i=0;i<segments1.size();i++){
          Block tmpblk = mem.getBlock(i);
          if(getBlockMinCount(tuples1.get(i),rField,min1)==tmpblk.getNumTuples()){
            flag1 = true;
            break;    // As long as we find one of the block of relation 1 meet the condition, quit loop
          }
        }

        for(int i=0;i<segments2.size();i++){
          Block tmpblk = mem.getBlock(i+segments1.size());
          if(getBlockMinCount(tuples2.get(i),sField,min1)==tmpblk.getNumTuples()){
            flag2 = true;
            break;  // As long as we find one of the block of relation 2 meet the condition, quit loop
          }
        }
        if(flag1 && !flag2)
          tuples1=deleteMin(tuples1,rField,min1);

        if(!flag1 && flag2)
          tuples2=deleteMin(tuples2,sField,min2);

        // Normal process
        if((flag1 && flag2) || (!flag1 && !flag2)){
          tuples1=deleteMin(tuples1,rField,min1);
          tuples2=deleteMin(tuples2,sField,min2);
        }
      }else if(min1!=null&&min2!=null){
        if(isInteger(min1)&&isInteger(min2)){
          if((Integer.parseInt(min1)-Integer.parseInt(min2))<0)
            tuples1=deleteMin(tuples1,rField,min1);
          else
            tuples2=deleteMin(tuples2,sField,min2);
        }else{
          if(min1.compareTo(min2)<0)
            tuples1=deleteMin(tuples1,rField,min1);
          else
            tuples2=deleteMin(tuples2,sField,min2);
        }
      }
    }
    return output;
  }

  public static List<String> readTxtFile(File file){
    List<String> ListToReturn = new ArrayList<String>();
    try {
      String encoding="UTF-8";
      // detetmine if file exist
      if(file.isFile() && file.exists()){
        InputStreamReader read = new InputStreamReader(
                // check encoding
                new FileInputStream(file),encoding);
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;
        while((lineTxt = bufferedReader.readLine()) != null){
          ListToReturn.add(lineTxt);
        }
        read.close();

      }else{
        System.out.println("Can not find file, check your file path");
      }
    } catch (Exception e) {
      System.out.println("Error on reading file");
      e.printStackTrace();
    }
    return ListToReturn;
  }

  public static void readFile(SchemaManager schema_manager, Disk disk, MainMemory mem, String filename) {
    File file = new File(filename);
    BufferedReader reader = null;
    try{
      reader = new BufferedReader(new FileReader(file));
      String command = null;
      long start = 0;
      long elapseTime = 0;
      double calElapsedTime = 0;
      long elapsedIO = 0;
      while((command=reader.readLine())!=null){
        start = System.currentTimeMillis();
        calElapsedTime = disk.getDiskTimer();
        elapsedIO = disk.getDiskIOs();

        String[] parts = command.split(" ");
        switch(parts[0]){
          case "SELECT":
//            System.out.println("select");
            selectHandler(schema_manager, mem, command, 0);
            break;
          case "INSERT":
//            System.out.println("insert");
            insertHandler(schema_manager, mem, command);
            break;
          case "DELETE":
//            System.out.println("delete");
            deleteHandler(schema_manager, mem, command);
            break;
          case "DROP":
//            System.out.println("drop");
            dropHandler(schema_manager,command);
            break;
          case "CREATE":
//            System.out.println("Create");
            createHandler(schema_manager,command);
            break;
        }
        elapseTime = System.currentTimeMillis()-start;

        System.out.print("Elapse time = " + elapseTime + " ms" + "\n");
        System.out.print("Calculated elapse time = " + (disk.getDiskTimer()-calElapsedTime) + " ms" + "\n");
        System.out.println("Disk I/Os = " + (disk.getDiskIOs()-elapsedIO) + "\n");
      }
      reader.close();
    }catch(Exception e){
      System.out.print("Error on I/O");
    }finally {
      if (reader != null) {
        try {
            reader.close();
        } catch (Exception et) {
          System.out.print("Error on read close");
        }
      }
    }
  }

  public static Relation selectHandler(SchemaManager schema_manager, MainMemory mem, String cmd, int subquery){
    cmd = cmd.replace(",","");
    ArrayList<String> relationName = new ArrayList<String>();
    ArrayList<String> orderField = new ArrayList<String>();
    int wherePos = 0, fromPos = 0, orderPos = 0, position=0;
    
    String[] parts = cmd.split(" ");
    // Find position of keyword FROM
    for(int i=0;i<parts.length;i++){
        if(parts[i].equals("FROM")){
          fromPos = i;
          break;
        }
    }
    // Find position of keyword WHERE
    for(int i=0;i<parts.length;i++){
      if(parts[i].equals("WHERE")){
        wherePos = i;
        break;
      }
    }
    // Find position of keyword ORDER
    for(int i=0;i<parts.length;i++){
      if(parts[i].equals("ORDER")){
        orderPos = i;
        break;
      }
    }
    // Find relations
    if(wherePos>0){
      for(position=fromPos+1;position<wherePos;position++){
        if(!parts[position].equals(",")){
          relationName.add(parts[position]);
        }
      }
    }else if(orderPos>0){
      for(position=fromPos+1;position<orderPos;position++){
        if(!parts[position].equals(",")){
          relationName.add(parts[position]);
        }
      }
    }else{
      for(position=fromPos+1;position<parts.length;position++){
        if(!parts[position].equals(",")){
          relationName.add(parts[position]);
        }
      }
    }
    if(orderPos>0){
      // Get sort field and sort relation
      String key = parts[orderPos+2];
      if(key.contains("\\.")){
        // Pattern like relation.field
        System.out.println("SELECT: sort by field "+key);
        String[] tmp = key.split("\\.");
        orderField.add(tmp[1]);
      }else{
        System.out.println("SELECT: sort by field "+key);
        orderField.add(key);
      }
    }
    if(parts[1].equals("*")){//select all
      System.out.println("SELECT: all\n");
      if(wherePos==0){
        // No condition
        System.out.println("SELECT: select with No condition\n");
        if(relationName.size()==1){
          /* Select on one relation */
          System.out.println("SELECT: from one relation\n");
          Relation relation = schema_manager.getRelation(relationName.get(0));
          if(subquery==0){
            if(orderPos==0){
              System.out.println("SELECT: plain table scan\n");
                /* If not a subquery and no order, plain table scan */
                for(int i=0;i<relation.getNumOfBlocks();i++){
                  relation.getBlock(i,0);
                  System.out.println(mem.getBlock(0));
                }
                return null;
              }
              else{
                /* If is order by */
                System.out.println("SELECT: ordered table scan\n");
                executeOrder(schema_manager,mem,relation,orderField,0);
                return null;
              }
            }else{
              if(orderPos==0){
                return relation;
              }else {
                relation = executeOrder(schema_manager,mem,relation,orderField, 1);
                return relation;
              }
          }
        }else if(relationName.size()>1){
          /* Join of Multiple relations */
          if(subquery==0){
            if(orderPos==0){
//              executeCrossJoin(schema_manager,mem,relationName,0);
              executeNaturalJoin(schema_manager,mem,relationName.get(0),relationName.get(1),"b",0);
              return null;
              // mode 0 means the join result can be printed immediately
            }else{
              Relation relation = executeCrossJoin(schema_manager,mem,relationName,1);
              executeOrder(schema_manager,mem,relation,orderField,0);
              return null;
            }
          }
          else{
            if(orderPos==0){
              Relation relation = executeCrossJoin(schema_manager,mem,relationName,1);
              return  relation;
              // Need to be written back to disk
            }else{
              Relation relation = executeCrossJoin(schema_manager,mem,relationName,1);
              relation = executeOrder(schema_manager,mem,relation,orderField,0);
              return relation;
            }
          }
        }
      }else if(wherePos>0){
        // With condition
        System.out.println("SELECT: select With conditions\n");
      }
    }else if(parts[1].equals("DISTINCT")){  /* For case: select distinct ...*/
      if(parts[2].equals("*")){
        System.out.println("SELECT: DISTINCT all\n");
        if(fromPos>0 && wherePos==0){
          // No condition
          System.out.println("No condition\n");
          if(relationName.size()==1){
            ArrayList<Tuple> output = new ArrayList<Tuple>();
            Relation relation = schema_manager.getRelation(relationName.get(0));
            // Only select one relation
            System.out.println("SELECT: DISTINCT from one relation\n");
            if(subquery==0) {
              if (orderPos == 0) {
                executeDistinct(schema_manager,mem,relation,relation.getSchema().getFieldNames(),0);
                return null;
              } else {
                relation = executeDistinct(schema_manager,mem,relation,relation.getSchema().getFieldNames(),1);
                executeOrder(schema_manager,mem,relation,orderField,0);
                return null;
              }
            }else{
              if (orderPos == 0) {
                relation = executeDistinct(schema_manager, mem, relation, relation.getSchema().getFieldNames(), 1);
                return relation;
              }else{
                relation = executeDistinct(schema_manager,mem,relation,relation.getSchema().getFieldNames(),1);
                relation = executeOrder(schema_manager,mem,relation,orderField,0);
                return relation;
              }
            }
          }else if(relationName.size()>1){
            /* Join of Multiple relations */
            if(subquery==0){
              if(orderPos==0){
                Relation relation = executeCrossJoin(schema_manager,mem,relationName,1);
                executeDistinct(schema_manager,mem,relation,relationName,0);
                return null;
                // mode 0 means the join result can be printed immediately
              }else{
                Relation relation = executeCrossJoin(schema_manager,mem,relationName,1);
                relation = executeDistinct(schema_manager,mem,relation,relationName,1);
                executeOrder(schema_manager,mem,relation,orderField,0);
                return null;
              }
            }
            else{
              if(orderPos==0){
                Relation relation = executeCrossJoin(schema_manager,mem,relationName,1);
                relation = executeDistinct(schema_manager,mem,relation,relationName,1);
                return  relation;
                // Need to be written back to disk
              }else{
                Relation relation = executeCrossJoin(schema_manager,mem,relationName,1);
                relation = executeDistinct(schema_manager,mem,relation,relationName,1);
                relation = executeOrder(schema_manager,mem,relation,orderField,1);
                return relation;
              }
            }
          }
        }else if(wherePos>0){
          // With condition
          System.out.println("With condition\n");
          return null;
        }
      }else{
        return null;
      }
    }else{
      return null;
    }
    return null;
  }

  public static void createHandler(SchemaManager schema_manager,String cmd){
    cmd = sanitize(cmd);
    String[] parts = cmd.split(" ");
    String relationName = parts[2];
    ArrayList<String> fieldName = new ArrayList<String>();
    ArrayList<FieldType> fieldType = new ArrayList<FieldType>();

    for(int i=3;i<parts.length;i++){
      if(i%2==1) fieldName.add(parts[i]);
      if(i%2==0){
          if(parts[i].equals("INT")) fieldType.add(FieldType.INT);
          else if(parts[i].equals("STR20")) fieldType.add(FieldType.STR20);
      }
    }

    if(fieldName.size()!=fieldType.size()){
      System.out.println("CREATE: Mismatch of fields and types\n");
    }else{
      Schema schema = new Schema(fieldName,fieldType);
      Relation relation = schema_manager.createRelation(relationName,schema);
      System.out.println("CREATE: successfully created relation "+relationName+"\n");
    }
  }

  public static void insertHandler(SchemaManager schema_manager, MainMemory mem, String cmd){
    cmd = sanitize(cmd);
    String[] parts = cmd.split(" ");
    String relationName = parts[2];
    ArrayList<String> fieldName = new ArrayList<String>();
    ArrayList<String> value = new ArrayList<String>();
    int valuePos=0, selectPos=0;

    for(int i=3;i<parts.length;i++){
      if(parts[i].equals("VALUES")){
        valuePos = i;
      }else if(parts[i].equals("SELETCT")){
        selectPos = i;
      }
    }
    /*case for insert into ... values ...*/
    if(valuePos>0){
      System.out.println("INSERT: insert into values to "+relationName+"\n");
      for(int i=3;i<valuePos;i++){
        fieldName.add(parts[i]);
      }
      for(int i=valuePos+1;i<parts.length;i++){
        value.add(parts[i]);
      }
      if(fieldName.size()!=value.size()){
        // System.out.print("Field name size: "+fieldName.size()+" field value size: "+value.size()+"\n");
        System.out.println("INSERT: Mismatch of fields and values\n");
      }else{
        Relation relation = schema_manager.getRelation(relationName);
        //Construct a tuple
        Tuple tuple = relation.createTuple();
        for(int i=0;i<fieldName.size();i++){
          if(isInteger(value.get(i)))
            tuple.setField(fieldName.get(i),Integer.parseInt(value.get(i)));
          else
            tuple.setField(fieldName.get(i),value.get(i));
        }
        //Append tuple to block
        appendTuple(relation,mem,0,tuple);
      } 
    }
    //case for insert into ... select ...
  }

  public static void deleteHandler(SchemaManager schema_manager, MainMemory mem, String cmd){
    cmd = sanitize(cmd);
    String[] parts = cmd.split(" ");
    String relationName = parts[2];
    Relation relation = schema_manager.getRelation(relationName);

    if(parts.length==3){
      //case for delete from r
      relation.deleteBlocks(0);
    }else if(parts.length>3){
      //case for delete from r where ...
      String fieldName = parts[4];
      String value = parts[6];

      Block block = null;
      Tuple tuple = null;

      for(int i=0;i<relation.getNumOfBlocks();i++){
        relation.getBlock(i,0);
        block = mem.getBlock(0);

        for(int j=0;j<block.getNumTuples();j++){
          tuple = block.getTuple(j);
          if(tuple.getField(fieldName).equals(value))
            block.invalidateTuple(j);
        }
        relation.setBlock(i,0);
      }
      System.out.println("DELETE: successfully delete\n");
    }
  }

  public static void dropHandler(SchemaManager schema_manager, String cmd){
    String[] parts = cmd.split(" ");
    String relationName = parts[2];

    schema_manager.deleteRelation(relationName);
    System.out.println("DROP: Successfully drop relation "+relationName+"\n");
  }

public static void insertFromSel(SchemaManager schema_manager, MainMemory mem, Relation destRel, ArrayList<String> fields, Relation srcRel){
    /* New Schema */
  Schema new_schema = getNewSchema(srcRel, fields);
  if(schema_manager.relationExists("selectfromsel"))
      schema_manager.deleteRelation("selectfromsel");

    Relation relation = schema_manager.createRelation("selectfromsel",new_schema);
    Tuple tuple = relation.createTuple();
    ArrayList<Tuple> output = new ArrayList<>();

    for(int i=0;i<srcRel.getNumOfBlocks();i++){
      srcRel.getBlock(i,0);
      Block block = mem.getBlock(0);
      for(Tuple t:block.getTuples()){
        for(int j=0;j<fields.size();j++){
          if(t.getField(fields.get(j)).type==FieldType.INT)
            tuple.setField(j,Integer.parseInt(t.getField(fields.get(j)).toString()));
          else
            tuple.setField(j,t.getField(fields.get(j)).toString());
        }
        output.add(tuple);
        tuple = relation.createTuple();
      }
    }
          
    // Insert back to disk
    int blk_cnt = destRel.getNumOfBlocks();
    Block blk = mem.getBlock(0);
    while(!output.isEmpty()){
      blk.clear();
      for(int i=0;i<new_schema.getTuplesPerBlock();i++){
        if(!output.isEmpty()){
          Tuple t = output.get(0);
          blk.setTuple(i,t);
          output.remove(t);
        }
      }
      destRel.setBlock(blk_cnt++,0);
    }
  }
}