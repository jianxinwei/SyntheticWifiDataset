package Anonymize;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Anonym{
    public static void main(String args[]) {
//        String input_single = "./input_single.csv";
//        String input_joint = "./input_joint.csv";
//        Mapping.mapSingle("./input_single.csv");
//        Mapping.mapSingle_Ordered("./input_single_order.csv");
//        Mapping.mapJoint("./input_joint.csv");
//        Mapping.mapJoint_Ordered("./input_joint_order.csv");
//        Mapping.Generalization("./input_general.csv");
    }


    public  static void mapSingle(String Filename){
        File inFile = new File(Filename);
        try {
            BufferedReader readFile = new BufferedReader(new FileReader(inFile));//read input_single
            String line = "", file_single = "", Table_mapped = "";
            File outDir = new File("./output");
            if(!outDir.exists()){
                outDir.mkdirs();
            }
            int file_count = 0;
            while((line = readFile.readLine())!= null){
                ++file_count;
                String cols[] = line.split(",");
                file_single = cols[0];
                File inFile1 = new File(file_single); // read CSV file

                try {
                    int par1 = -1, par2 = -1;
                    par1 = file_single.lastIndexOf("\\");
                    par2 = file_single.lastIndexOf("/");
                    if(par1!=-1) Table_mapped = file_single.substring(0, par1-1)+"\\output\\"+ file_single.substring(par1+1);
                    if(par2!=-1) Table_mapped = file_single.substring(0, par2-1)+"/output/"+ file_single.substring(par2+1);
                    if(par1==-1 && par2==-1) System.out.println("Input File"+file_count+"Wrong");
                    File mapped = new File(Table_mapped);
                    BufferedReader reader1 = new BufferedReader(new FileReader(inFile1));
                    BufferedWriter writer1 = new BufferedWriter(new FileWriter(mapped));

                    String titles = "", map_str="", mapTable="", data_mapped = "";
                    int col = 0, m = 0;
                    titles = reader1.readLine();//read
                    writer1.write(titles);
                    writer1.newLine();
                    String item[] = titles.split(",");//read title of each column
                    String tmpString[] = new String[item.length];
                    String inString = "";

                    HashMap<String, Integer> map[] = new HashMap[cols.length-1];
                    File outFile_mapTable[] = new File[cols.length-1];
                    BufferedWriter writer[] = new BufferedWriter[cols.length-1];
                    for(m=0; m<cols.length -1; ++m){
                        col = Integer.parseInt(cols[m+1]);
                        map[m] = new HashMap();
                        mapTable = "./output/"+item[col]+"_"+file_count+".csv";
                        outFile_mapTable[m] = new File(mapTable);
                        writer[m] = new BufferedWriter(new FileWriter(outFile_mapTable[m]));
                    }

                    while ((inString = reader1.readLine())!= null){
                        tmpString = inString.split(",");
                        for(m=0; m<cols.length-1; ++m){
                            col = Integer.parseInt(cols[m+1]);//No of column
                            // System.out.println(tmpString[col1]);
                            if (!map[m].containsKey(tmpString[col])){
                                map_str = tmpString[col] + "," + map[m].size();
//                                System.out.println(map_str);
                                writer[m].write(map_str);
                                writer[m].newLine();
                                map[m].put(tmpString[col], map[m].size());
                            }
                            tmpString[col] = String.valueOf(map[m].get(tmpString[col]));
                        }
                        data_mapped = String.join(",",tmpString);
//                        System.out.println(data_mapped);
                        writer1.write(data_mapped);
                        writer1.newLine();
                    }
                    reader1.close();
                    writer1.close();
                    //关闭读写文件
                    for(m=0; m<cols.length -1; ++m){
                        writer[m].close();
                    }
                } catch (FileNotFoundException ex) {
                    System.out.println("Data File not found when singly map");
                } catch (IOException ex) {
                    System.out.println("Error when read or write");
                }
            }
        } catch (FileNotFoundException ex) {
            System.out.println("Data File not found when singly map");
        } catch (IOException ex) {
            System.out.println("Error when read or write");
        }
    }

    public static void mapJoint(String Filename) {
        File inFile = new File(Filename);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inFile));//read input

            int col = 0, file_count = 0;
            String clus_str = "", cluster_name = "", file_col[] = new String[2], filename_str = "";
            String col_str = "", tuple = "", titles = "", map_str = "", Table_mapped="", data_mapped="";
            int par1=-1, par2=-1, par3=-1;
            while ((clus_str = reader.readLine()) != null) {
                String items[] = clus_str.split(",");
                cluster_name = items[0];//name of outputFile
//                System.out.println(cluster_name);
                File outDir = new File("./output");
                if(!outDir.exists()){
                    outDir.mkdirs();
                }
                File outFile = new File("./output/"+cluster_name + "_uni.csv");
                HashMap<String, Integer> map1 = new HashMap<String, Integer>();

                //start mapping tables
                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
                    for (int i = 1; i < items.length; ++i) {
                        file_col = items[i].split(";");
                        filename_str = file_col[0];
//                        if(!files.contains(filename_str)){
//                            files.add(filename_str);
//                        }
                        
                        par1 = filename_str.lastIndexOf("\\");
//                        if(par1!=-1) Table_mapped = filename_str.substring(0, par1-1)+"\\output\\tmp.csv";
//                        else Table_mapped = filename_str+"tmp.csv";
                        Table_mapped = "./output/tmp.csv";
                        System.out.println(Table_mapped);
                        String file_copyString;
//                        file_copyString = filename_str.substring(0, par1-1)+"\\output\\"+filename_str.substring(par1+1);
                        file_copyString = "./output/" + filename_str.substring(par1+1);
                        System.out.println(file_copyString);
                        File copyFile = new File(file_copyString);
                        if (copyFile.exists()) {
                            copyFile.delete();
                        }
                        File tmp_File = new File(Table_mapped);
                        BufferedWriter tmp_writer = new BufferedWriter(new FileWriter(tmp_File));

                        col_str = file_col[1];
                        col = Integer.parseInt(col_str);
    //                    System.out.println(col);
                        File inFile1 = new File(filename_str);
                        Files.copy(inFile1.toPath(), copyFile.toPath());
                        BufferedReader reader1 = new BufferedReader(new FileReader(copyFile));
                        titles = reader1.readLine();//read the headline
                        tmp_writer.write(titles);
                        tmp_writer.newLine();
                        String item[] = titles.split(",");//read title of each column
                        String tmpString[] = new String[item.length];
                        //map tuples in table
                        while ((tuple = reader1.readLine()) != null) {
                            tmpString = tuple.split(",");
//                            System.out.println(tmpString[col]);
                            if (!map1.containsKey(tmpString[col])) {
                                map_str = tmpString[col] + "," + map1.size();
//                                System.out.println(map_str);
                                writer.write(map_str);
                                writer.newLine();
                                map1.put(tmpString[col], map1.size());
                            }
                            tmpString[col] = String.valueOf(map1.get(tmpString[col]));
                            data_mapped = String.join(",",tmpString);
//                        System.out.println(data_mapped);
                            tmp_writer.write(data_mapped);
                            tmp_writer.newLine();
                        }
                        reader1.close();
                        tmp_writer.close();
                        replace(tmp_File, copyFile);
                    }
                    writer.close();
                } catch (FileNotFoundException ex) {
                    System.out.println("Data File not found");
                } catch (IOException ex) {
                    System.out.println("Error when read or write DataFile");
                }
            }
            reader.close();

        } catch (FileNotFoundException ex) {
            System.out.println("input File not found in mapJoint");
        } catch (IOException ex) {
            System.out.println("Error when read or write inputFile");
        }
    }

    public static void replace(File old, File newName) {
        if(newName.exists()){
            newName.delete();
        }
        if(old.renameTo(newName)) {
            System.out.println(old.toString()+ " update!");
        } else {
            System.out.println("Error");
        }
    }

    public  static void mapSingle_Ordered(String Filename){
        File inFile = new File(Filename);
        try {
            BufferedReader readFile = new BufferedReader(new FileReader(inFile));//read input_single
            String line = "", file_single = "", Table_mapped = "";
            File outDir = new File("./output");
            if(!outDir.exists()){
                outDir.mkdirs();
            }
            int file_count = 0;
            while((line = readFile.readLine())!= null){
                ++file_count;
                String cols[] = line.split(",");
                file_single = cols[0];
                File inFile1 = new File(file_single); // read CSV file

                //rank columns and save their maps
                ArrayList<Integer> cols_order[]=new ArrayList[cols.length-1];
                try {
                    BufferedReader read_order = new BufferedReader(new FileReader(inFile1));
                    String titles = "", map_str="", mapTable="", data_mapped = "";
                    int col = 0, m = 0;
                    titles = read_order.readLine();//read
                    String item[] = titles.split(",");//read title of each column
                    String tmpString[] = new String[item.length];
                    String inString = "";
                    int str2int;
                    for(m=0; m<cols.length -1; ++m){
                        col = Integer.parseInt(cols[m+1]);
                        cols_order[m] = new ArrayList<Integer>();
                    }
                    while ((inString = read_order.readLine())!=null){
                        tmpString = inString.split(",");
                        //collect all values of each column
                        for(m=0; m<cols.length-1; ++m){
                            col = Integer.parseInt(cols[m+1]);
                            str2int = Integer.parseInt(tmpString[col]);
                            if(!cols_order[m].contains(str2int)){
                                cols_order[m].add(str2int);
                            }
                        }
                    }
                    for(m=0; m<cols.length-1; ++m){
                        Collections.sort(cols_order[m]);
                    }
                    read_order.close();
                } catch (FileNotFoundException ex) {
                    System.out.println("Data File not found when singly map");
                } catch (IOException ex) {
                    System.out.println("Error when read or write");
                }

                //map data
                try {
                    int par1 = -1, par2 = -1;
                    par1 = file_single.lastIndexOf("\\");
                    par2 = file_single.lastIndexOf("/");
                    if(par1!=-1) Table_mapped = file_single.substring(0, par1-1)+"\\output\\"+ file_single.substring(par1+1);
                    if(par2!=-1) Table_mapped = file_single.substring(0, par2-1)+"/output/"+ file_single.substring(par2+1);
                    if(par1==-1 && par2==-1) System.out.println("Input File"+file_count+"Wrong");
                    File mapped = new File(Table_mapped);

                    BufferedReader reader1 = new BufferedReader(new FileReader(inFile1));
                    BufferedWriter writer1 = new BufferedWriter(new FileWriter(mapped));

                    String titles = "", map_str="", mapTable="", data_mapped = "";
                    int col = 0, m = 0;
                    titles = reader1.readLine();//read
                    writer1.write(titles);
                    writer1.newLine();
                    String item[] = titles.split(",");//read title of each column
                    String tmpString[] = new String[item.length];
                    String inString = "";
                    HashMap<String, Integer> map[] = new HashMap[cols.length-1];
                    File outFile_mapTable[] = new File[cols.length-1];
                    BufferedWriter writer[] = new BufferedWriter[cols.length-1];
                    for(m=0; m<cols.length -1; ++m){
                        col = Integer.parseInt(cols[m+1]);
                        mapTable = "./output/"+item[col]+"_"+file_count+".csv";
                        map[m] = new HashMap<String, Integer>();
                        outFile_mapTable[m] = new File(mapTable);
                        writer[m] = new BufferedWriter(new FileWriter(outFile_mapTable[m]));
                    }

                    int mapValue;
                    while ((inString = reader1.readLine())!= null){
                        tmpString = inString.split(",");
                        for(m=0; m<cols.length-1; ++m){
                            col = Integer.parseInt(cols[m+1]);//No of column
                            // System.out.println(tmpString[col1]);
                            mapValue = cols_order[m].indexOf(Integer.parseInt(tmpString[col]));
                            if(!map[m].containsKey(tmpString[col])) {
                                map[m].put(tmpString[col], mapValue);
                                map_str = tmpString[col] + "," + mapValue;
                                //System.out.println(map_str);
                                writer[m].write(map_str);
                                writer[m].newLine();
                            }
                            tmpString[col] = String.valueOf(mapValue);
                        }
                        data_mapped = String.join(",",tmpString);
//                        System.out.println(data_mapped);
                        writer1.write(data_mapped);
                        writer1.newLine();
                    }
                    reader1.close();
                    writer1.close();
                    //关闭读写文件
                    for(m=0; m<cols.length -1; ++m){
                        writer[m].close();
                    }
                } catch (FileNotFoundException ex) {
                    System.out.println("Data File not found when singly map");
                } catch (IOException ex) {
                    System.out.println("Error when read or write");
                }
            }
        } catch (FileNotFoundException ex) {
            System.out.println("Data File not found when singly map");
        } catch (IOException ex) {
            System.out.println("Error when read or write");
        }
    }

    public static void mapJoint_Ordered(String Filename) {
        File inFile = new File(Filename);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inFile));//read input

            int col = 0, file_count = 0;
            String clus_str = "", cluster_name = "", file_col[] = new String[2], filename_str = "";
            String col_str = "", tuple = "", titles = "", map_str = "", Table_mapped="", data_mapped="";
            int par1=-1, par2=-1;
            while ((clus_str = reader.readLine()) != null) {
                String items[] = clus_str.split(",");
                cluster_name = items[0];//name of outputFile
//                System.out.println(cluster_name);
                File outDir = new File("./output");
                if(!outDir.exists()){
                    outDir.mkdirs();
                }
                File outFile = new File("./output/"+cluster_name + "_uni.csv");
                HashMap<String, Integer> map1 = new HashMap<String, Integer>();
                //rank column
                int f = 0, col_int, str2int;
                ArrayList<Integer> cols_order=new ArrayList<>();
                for(f=1; f<items.length; ++f){
                    file_col = items[f].split(";");
                    filename_str = file_col[0];
                    col_int = Integer.parseInt(file_col[1]);
                    File inFile_order = new File(filename_str);
                    try {
                        BufferedReader read_order = new BufferedReader(new FileReader(inFile_order));

                        titles = read_order.readLine();//read the headline
                        String attributes[] = titles.split(",");//read title of each column
                        String tmpString[] = new String[attributes.length];
                        //map tuples in table
                        while ((tuple = read_order.readLine()) != null) {
                            tmpString = tuple.split(",");
                            str2int = Integer.parseInt(tmpString[col_int]);
                            if (!cols_order.contains(str2int)) {
                                cols_order.add(str2int);
                            }
                        }
                        read_order.close();
                    } catch (FileNotFoundException ex) {
                        System.out.println("Data File not found");
                    } catch (IOException ex) {
                        System.out.println("Error when read or write DataFile");
                    }
                }
                Collections.sort(cols_order);

                //start mapping tables
                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
                    for (int i = 1; i < items.length; ++i) {
                        file_col = items[i].split(";");
                        filename_str = file_col[0];
//                        if(!files.contains(filename_str)){
//                            files.add(filename_str);
//                        }

                        par1 = filename_str.lastIndexOf("\\");
//                        if(par1!=-1) Table_mapped = filename_str.substring(0, par1)+"tmp.csv";
//                        else Table_mapped = filename_str+"tmp.csv";
                        Table_mapped = "./output/tmp.csv";
                        File tmp_File = new File(Table_mapped);
                        BufferedWriter tmp_writer = new BufferedWriter(new FileWriter(tmp_File));
                        String file_copyString;
                        file_copyString = "./output/" + filename_str.substring(par1+1);
//                        System.out.println(file_copyString);
                        File copyFile = new File(file_copyString);
                        if (copyFile.exists()) {
                            copyFile.delete();
                        }
                        col_str = file_col[1];
                        col = Integer.parseInt(col_str);
                        //                    System.out.println(col);
                        File inFile1 = new File(filename_str);
                        Files.copy(inFile1.toPath(), copyFile.toPath());
                        BufferedReader reader1 = new BufferedReader(new FileReader(copyFile));
                        titles = reader1.readLine();//read the headline
                        tmp_writer.write(titles);
                        tmp_writer.newLine();
                        String item[] = titles.split(",");//read title of each column
                        String tmpString[] = new String[item.length];
                        //map tuples in table
                        int mapValue = 0;
                        while ((tuple = reader1.readLine()) != null) {
                            tmpString = tuple.split(",");
                            str2int = Integer.parseInt(tmpString[col]);
                            mapValue = cols_order.indexOf(str2int);
                            if(!map1.containsKey(tmpString[col])) {
                                map1.put(tmpString[col], mapValue);
                                map_str = tmpString[col] + "," + mapValue;
//                                System.out.println(map_str);
                                writer.write(map_str);
                                writer.newLine();
                            }
                            tmpString[col] = String.valueOf(mapValue);
                            data_mapped = String.join(",",tmpString);
//                        System.out.println(data_mapped);
                            tmp_writer.write(data_mapped);
                            tmp_writer.newLine();
                        }
                        reader1.close();
                        tmp_writer.close();
                        replace(tmp_File, copyFile);
                    }
                    writer.close();
                } catch (FileNotFoundException ex) {
                    System.out.println("Data File not found");
                } catch (IOException ex) {
                    System.out.println("Error when read or write DataFile");
                }
            }
            reader.close();

        } catch (FileNotFoundException ex) {
            System.out.println("input File not found");
        } catch (IOException ex) {
            System.out.println("Error when read or write inputFile");
        }
    }

    public  static void Generalization(String Filename){
        File inFile = new File(Filename);
        try {
            BufferedReader readFile = new BufferedReader(new FileReader(inFile));//read input_single
            String line = "", file_single = "", Table_mapped = "";
            File outDir = new File("./output");
            if(!outDir.exists()){
                outDir.mkdirs();
            }
            int file_count = 0;
            while((line = readFile.readLine())!= null){
                ++file_count;
                String cols[] = line.split(",");
                file_single = cols[0];
                File inFile1 = new File(file_single); // read CSV file
                int par1 = -1, par2 = -1;
                par1 = file_single.lastIndexOf(".");
//                    par2 = file_single.lastIndexOf("/");
//                    if(par1!=-1) Table_mapped = file_single.substring(0, par1-1)+"\\output\\"+ file_single.substring(par1+1);
//                    if(par2!=-1) Table_mapped = file_single.substring(0, par2-1)+"/output/"+ file_single.substring(par2+1);
//                    if(par1==-1 && par2==-1) System.out.println("Input File"+file_count+"Wrong");
                if(par1!=-1) Table_mapped = file_single.substring(0, par1)+"tmp.csv";
                else Table_mapped = file_single+"tmp.csv";
                File tmp_File = new File(Table_mapped);
                try {
                    BufferedReader reader1 = new BufferedReader(new FileReader(inFile1));
                    BufferedWriter writer1 = new BufferedWriter(new FileWriter(tmp_File));

                    String titles = "", map_str="", mapTable="", data_mapped = "";
                    int col = 0, m = 0;
                    titles = reader1.readLine();//read
                    writer1.write(titles);
                    writer1.newLine();
                    String item[] = titles.split(",");//read title of each column
                    String tmpString[] = new String[item.length];
                    String inString = "";

                    HashMap<String, String> map[] = new HashMap[cols.length-1];
                    File outFile_mapTable[] = new File[cols.length-1];
                    BufferedWriter writer[] = new BufferedWriter[cols.length-1];
                    int upper, floor, bandwidth = 1;
                    String range="", col_bandwidth[];
                    for(m=0; m<cols.length -1; ++m){
                        col_bandwidth = cols[m+1].split(";");//No of column
                        col = Integer.parseInt(col_bandwidth[0]);
                        map[m] = new HashMap();
                        mapTable = "./output/"+item[col]+"_"+file_count+".csv";
                        outFile_mapTable[m] = new File(mapTable);
                        writer[m] = new BufferedWriter(new FileWriter(outFile_mapTable[m]));
                    }
                    while ((inString = reader1.readLine())!= null){
                        tmpString = inString.split(",");
                        for(m=0; m<cols.length-1; ++m){
                            col_bandwidth = cols[m+1].split(";");//No of column
                            col = Integer.parseInt(col_bandwidth[0]);
                            bandwidth = Integer.parseInt(col_bandwidth[1]);
                            floor = Math.floorDiv(Integer.parseInt(tmpString[col]), bandwidth)*bandwidth;
                            upper = floor+bandwidth;
                            range = String.valueOf(floor)+"-"+String.valueOf(upper);
                            // System.out.println(tmpString[col1]);
                            if (!map[m].containsKey(tmpString[col])){
//                                map[m].put(tmpString[col], map[m].size());
                                map_str = tmpString[col] + "," + range;
//                                System.out.println(map_str);
                                writer[m].write(map_str);
                                writer[m].newLine();
                                map[m].put(tmpString[col], range);
                            }
                            tmpString[col] = range;
                        }
                        data_mapped = String.join(",",tmpString);
//                        System.out.println(data_mapped);
                        writer1.write(data_mapped);
                        writer1.newLine();
                    }
                    reader1.close();
                    writer1.close();
                    //关闭读写文件
                    for(m=0; m<cols.length -1; ++m){
                        writer[m].close();
                    }
                } catch (FileNotFoundException ex) {
                    System.out.println("Data File not found when singly map");
                } catch (IOException ex) {
                    System.out.println("Error when read or write");
                }
                replace(tmp_File, inFile1);
            }
        } catch (FileNotFoundException ex) {
            System.out.println("Data File not found when singly map");
        } catch (IOException ex) {
            System.out.println("Error when read or write");
        }
    }
}

//sha1 algorithm
class HashKit {
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    public static String sha1(String srcStr){
        return hash("SHA-1", srcStr);
    }

    public static String hash(String algorithm, String srcStr) {
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            byte[] bytes = md.digest(srcStr.getBytes("utf-8"));
            return toHex(bytes);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String toHex(byte[] bytes) {
        StringBuilder ret = new StringBuilder(bytes.length * 2);
        for (int i=0; i<bytes.length; i++) {
            ret.append(HEX_DIGITS[(bytes[i] >> 4) & 0x0f]);
            ret.append(HEX_DIGITS[bytes[i] & 0x0f]);
        }
        return ret.toString();
    }
}