import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

import java.io.File;
import java.util.List;
import java.util.Scanner;

public class Interface {

    public static void statementDisplay(List<Statement> list){
        for (Statement tmp_statement : list) {
            String tmp_content = tmp_statement.getContent();
            System.out.println(tmp_content);
            if (tmp_statement.getSubstatement() != null) {
                System.out.println(">>>new list");
                statementDisplay(tmp_statement.getSubstatement());
                System.out.println(">>>end new list");
            }
        }
    }

    public static void main(String[] args){
        boolean not_exit = true;
        QueryParser parser = new QueryParser();
        Scanner in = new Scanner(System.in);
        //storageManager parameter
        MainMemory memory=new MainMemory();
        Disk disk=new Disk();
        SchemaManager schema_manager=new SchemaManager(memory,disk);
        QueryExecutor executor = new QueryExecutor(memory, disk, schema_manager);
        System.out.println("************************************************");
        System.out.println("*           The TinySql of CSCE608               *");
        System.out.println("*             Aoran Xu: 927001477                *");
        System.out.println("*           Xiaoquan Xing: 626008824             *");
        System.out.println("************************************************");
        while (not_exit) {
            System.out.println("-------------You have three choices-------------");
            System.out.println("1.Input queries");
            System.out.println("2.Read a file");
            System.out.println("3.Exit now");
            System.out.println("Give me your choice:");
            String res = in.nextLine();
            switch (res) {
                case "1":
//                    System.out.println("-----------------Query------------------");
                    System.out.println("Please input query or just input 'quit' to remake choice:");
                    String query =  in.nextLine();
                    while(!query.equalsIgnoreCase("quit"))
                    {
                        //System.out.println("The query you just input is "+query);
                        Statement parsed_query = parser.parse(query);
                        executor.execute(parsed_query);

//                        System.out.println("--------------------Command Line Mode------------------");
                        System.out.println("Please input query or just input 'quit' to remake choice:");
                        query =  in.nextLine();
                    }
                    break;
                case "2":
//                    System.out.println("-----------------File Mode---------------");
                    System.out.println("Please input file name or just input 'quit' to remake choice:");
                    String file_name = in.nextLine();

                    while(!file_name.equalsIgnoreCase("quit")) {
                        //read querys in the file
                        File file = new File(file_name);
                        List<String> query_list = ParserHelper.readTxtFile(file);
                        for (String s: query_list) {
                            Statement tmp_query = parser.parse(s);
                            executor.execute(tmp_query);
                        }
//                        System.out.println("-----------------File Mode---------------");
                        System.out.println("Please input file name or just input 'quit' to remake choice:");
                        file_name = in.nextLine();
                    }

                    break;

                case "3":
                    not_exit = false;
                    break;
                default :
                    System.out.println("Please choose 1, 2, 3");
                    res = in.nextLine();
            }
        }
    }
}