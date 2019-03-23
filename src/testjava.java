import java.io.*;
import java.util.ArrayList;
import java.util.List;

//public class testjava {

//    public static Boolean isInteger(String s){
//        try {
//            Integer.parseInt(s);
//        } catch (NumberFormatException e) {
//            return false;
//        } catch (NullPointerException e) {
//            return false;
//        }
//        return true;
//    }


//    public static int twoPassPhaseOne(Relation relation, MainMemory mem, final ArrayList<String> indexField){
//        int readIn=0, sortedBlocks = 0;
//        while(sortedBlocks<relation.getNumOfBlocks()){
//            readIn = ((relation.getNumOfBlocks()-sortedBlocks)>mem.getMemorySize())?mem.getMemorySize():(relation.getNumOfBlocks()-sortedBlocks);
//            relation.getBlocks(sortedBlocks,0,readIn);
//            ArrayList<Tuple> tuples = mem.getTuples(0,readIn);
//            Collections.sort(tuples,new Comparator<Tuple>(){
//                public int compare(Tuple t1, Tuple t2){
//                    int[] result = new int[indexField.size()];
//                    for(int i=0;i<indexField.size();i++){
//                        String v1 = t1.getField(indexField.get(i)).toString();
//                        String v2 = t2.getField(indexField.get(i)).toString();
//                        if(isInteger(v1) && isInteger(v2)){
//                            result[i] = Integer.parseInt(v1)-Integer.parseInt(v2);
//                        }
//                        else
//                            result[i] = v1.compareTo(v2);
//                    }
//                    // Return 0 when all fields equal, 1 when t1>t2, -1 when t1<t2
//                    for(int i=0;i<indexField.size();i++){
//                        if(result[i]>0) return 1;
//                        else if(result[i]<0) return -1;
//                    }
//                    return 0;
//                }
//            });
//            mem.setTuples(0,tuples);
//            //System.out.print("Now the memory contains: " + "\n");
//            //System.out.print(mem + "\n");
//            relation.setBlocks(sortedBlocks,0,readIn);
//            sortedBlocks+=readIn;
//        }
//        return readIn;
//    }

//    public static List<String> readTxtFile(File file){
//        List<String> ListToReturn = new ArrayList<String>();
//        try {
//            String encoding="UTF-8";
//            // detetmine if file exist
//            if(file.isFile() && file.exists()){
//                InputStreamReader read = new InputStreamReader(
//                        // check encoding
//                        new FileInputStream(file),encoding);
//                BufferedReader bufferedReader = new BufferedReader(read);
//                String lineTxt = null;
//                while((lineTxt = bufferedReader.readLine()) != null){
//                    ListToReturn.add(lineTxt);
//                }
//                read.close();
//
//            }else{
//                System.out.println("Can not find file, check your file path");
//            }
//        } catch (Exception e) {
//            System.out.println("Error on reading file");
//            e.printStackTrace();
//        }
//        return ListToReturn;
//    }

//    public static List<String> fileReader(File file){
//        BufferedReader reader = null;
//        try{
//            reader = new BufferedReader(new FileReader(file));
//            List<String> inputLines=new ArrayList<String>();
//            String cmd;
//            while((cmd=reader.readLine())!=null){
//                inputLines.add(cmd);
//            }
//            reader.close();
//            return inputLines;
//        }catch(IOException e){
//            e.printStackTrace();
//            return null;
//        }finally {
//            if(reader!=null)
//                try{
//                    reader.close();
//                }catch (IOException e) {
//                    e.printStackTrace();
//                }
//        }
//    }


//    public static void main(String[] args) {

//        test
//        String str = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)\n" +
//                "INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, \"A\")\n" +
//                "INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 100, 98, \"C\")\n";
//        System.out.print(str);
//        String str_re = ParserHelper.sanitize(str);
//        System.out.print("\n");
//        System.out.print(str_re);

//        File file = new File("test2.txt");
//        List<String> output = fileReader(file);
//        for(String s: output) {
//            System.out.print(s);
//        }
//        System.out.print("\n");
//        List<String> output2 = readTxtFile(file);
//        for(String s: output2) {
//            System.out.print(s);
//        }
//        System.out.print("\n");
//        System.out.print(output.size());
//        System.out.print(output2.size());
//        System.out.print("\n");
//        for(int i =0; i < output.size(); i++)
//        {
//            if(!output.get(i).equals(output2.get(i)))
//                System.out.print("wrong!");
//            else
//                System.out.print("correct!");
//        }
//    }

//}
