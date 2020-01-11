package generateWifi;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import java.io.*;

/**
 *
 * @author Jianxin Wei
 */
public class trace {
    private static final int oneDay = 24 * 3600;
    private static int Timeslot = 7200;
    private static String date = "2018-8-13";
    private static final int student_token = 1, ipaddress_token = 2, mac_token = 3, starttime = 4, endtime = 5, day_col
            = 6,
            bytesreceived_ord = 7,
            bytessent_ord = 8, apName_ord = 10, aploc = 11, aploc_map = 12, proto_col = 13, rssi_col = 14, ssr_col = 15,
            ssid_col = 16;
    private static ArrayList<String> wifi = new ArrayList<>();
    private static ArrayList<String> new_data = new ArrayList<>();
    private static Map<String, ArrayList<String>> StartPoints = new HashMap<>();
    private static int TotalPoints = 0;
    private static Table<String, String, ArrayList<String>> Transition = HashBasedTable.create();
    //Transition R: start ap, C:start time, V: next ap & time,

    public static void main(String args[]) {
//        int t=statistics.time2long("20180813 09:05:00.5");
//        System.out.println(t);
//        String[] fileList = {"D:\\NUS\\DataTweaking\\wifi\\test.csv"};
        String[] fileList = {"D:\\NUS\\DataTweaking\\wifi\\wifi_2018-08-14.csv"};
        rankStu(fileList);
        getTrack("D:\\NUS\\DataTweaking\\wifi\\ranked.csv");
        MarkovMatrix();
        collectStartPoints();
        generateTrack(1000);
        outputData("D:\\NUS\\DataTweaking\\wifi\\test_syn.csv");

    }

    //output the trajectory of one student
    public static void write_trajectory(String inputfile, String outputfile) {
        File read1 = new File(inputfile);
        File write1 = new File(outputfile);
        try {
            BufferedReader reader1 = new BufferedReader(new FileReader(read1));
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(write1));
            String headline = reader1.readLine();
            writer1.write(headline);
            writer1.newLine();
            String rowString, stuID = "57a5cafb8b3b305496650fa6d68d13e3abd3a102";
            while ((rowString = reader1.readLine()) != null) {
                String items[] = rowString.split(",");
                if (items[1].equals(stuID)) {
                    writer1.write(rowString);
                    writer1.newLine();
                }
            }
            reader1.close();
            writer1.close();

        } catch (FileNotFoundException ex) {
            System.out.println("File:" + inputfile + " not found when collecting");
        } catch (IOException ex) {
            System.out.println("Error when read or write trace");
        }
    }

    //store the data in our program
    public static void getTrack(String filename) {
        wifi.clear();
        try {
            String rowString;
            BufferedReader reader1 = new BufferedReader(new FileReader(filename));
            BufferedReader reader_date = new BufferedReader(new FileReader(filename));
            reader1.readLine();
            reader_date.readLine();
            while ((rowString = reader1.readLine()) != null) {
                wifi.add(rowString);
            }
            while ((rowString = reader_date.readLine()) != null) {
                date = rowString.split(",")[day_col];
                System.out.println(date);
                break;
            }
            reader1.close();
            reader_date.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when collecting");
        } catch (IOException ex) {
            System.out.println("Error when read or write inputFile");
        }
    }

    //rank the data by student token
    public static void rankStu(@NotNull String[] fileList) {
        int f = 0, i = 0, j = 0, rssi, ssr;
        String filename = "", headline = "", rowString = "";
        String apName_str = "", stuID = "", IP = "", macID = "", loc_str = "", loc_map_str = "", protocol = "", ssid
                = "";

        int start_int, end_int, received_long, sent_long;
        int flag1 = 0;

        if (fileList.length == 0) return;
        for (f = 0; f < fileList.length; ++f) {
            filename = fileList[f];
            File inFile1 = new File(filename);
            System.out.println("processing: " + inFile1.getName());
            try {
                BufferedReader reader1 = new BufferedReader(new FileReader(inFile1));
                BufferedWriter writer1 = new BufferedWriter(new FileWriter(inFile1.getParent() + "\\ranked.csv"));
                writer1.write(",student_token,ipaddress_token,mac_token,sessionstarttime,sessionendtime,day," +
                        "bytesreceived,bytessent,eaptype,apname,aplocation,aplocation_mapping,protocol,rssi,snr,ssid");
                writer1.newLine();
                headline = reader1.readLine();
//                String colName[] = headline.split(",");
                while ((rowString = reader1.readLine()) != null) {
                    String items[] = rowString.split(",");
                    flag1 = check_data(items);
                    if (flag1 != 0) continue;
                    wifi.add(rowString);
                }
                reader1.close();
                Comparator comp_stu = new Mycomparator_stu();
                Collections.sort(wifi, comp_stu);
                for (i = 0; i < wifi.size(); ++i) {
                    writer1.write(wifi.get(i));
                    writer1.newLine();
                }
                writer1.close();
            } catch (FileNotFoundException ex) {
                System.out.println("File:" + filename + " not found when collecting");
            } catch (IOException ex) {
                System.out.println("Error when read or write inputFile");
            }


            //clear the collection of current day and proceed to next day
//            apName_time_stu.clear();
//            apName_startend_stu.clear();
//            apName_time_bytes.clear();
//            apName_stu_mac.clear();
//            apName_ip.clear();
//            apName_loc.clear();
//            apName_protocol_ssid.clear();
//            apName_rssi_ssr.clear();
//            apName_rssi.clear();
//            apName_ssr.clear();
        }

    }

    //build transition matrix
    public static void MarkovMatrix() {
        String row_str, start_point, next_point;
        String items_current[], items_next[];
        int start_int, end_int;
        String matrix_column;

        //traverse data
        for (int i = 0; i + 1 < wifi.size(); ++i) {
            ArrayList<String> matrix_value = new ArrayList<>();// a list of time-pair

            row_str = wifi.get(i);
            items_current = row_str.split(",");
            row_str = wifi.get(i + 1);
            items_next = row_str.split(",");

            if (items_current[student_token].equals(items_next[student_token])) {
                start_int = time2int(items_current[starttime], items_current[day_col]);
                end_int = time2int(items_current[endtime], items_current[day_col]);
                start_point = items_current[aploc_map];
                next_point = items_next[aploc_map];
                matrix_column = time2interval(end_int);

                if (!Transition.contains(start_point, matrix_column)) {
                    Transition.put(start_point, matrix_column, matrix_value);
                } else {
                    matrix_value = Transition.get(start_point, matrix_column);
                }
                matrix_value.add(StringUtils.join(Arrays.copyOfRange(items_next, starttime, 17), ","));
//                matrix_value.add(items_next[starttime] + "," + items_next[endtime] + "," + next_point);
                Transition.put(start_point, matrix_column, matrix_value);
//                System.out.println(items[starttime]+","+items[endtime]+" "+i);
            }
        }
    }

    //collect the distribution of start points at campus
    public static void collectStartPoints() {
        String items_current[], items_next[];
        String row_str, startpoint, timepair;

        for (int i = 0; i < wifi.size(); i++) {
            ArrayList<String> times = new ArrayList<>();
            row_str = wifi.get(i);
            items_current = row_str.split(",");
            startpoint = items_current[aploc_map];
            timepair = items_current[starttime] + "," + items_current[endtime];

            if (i == 0) {
                if (StartPoints.containsKey(startpoint)) {
                    times = StartPoints.get(startpoint);
                }
                times.add(StringUtils.join(Arrays.copyOfRange(items_current, starttime, 17), ","));
                StartPoints.put(startpoint, times);
                TotalPoints += 1;
            } else {
                row_str = wifi.get(i - 1);
                items_next = row_str.split(",");
                if (!items_current[student_token].equals(items_next[student_token])) {
                    if (StartPoints.containsKey(startpoint)) {
                        times = StartPoints.get(startpoint);
                    }
                    times.add(StringUtils.join(Arrays.copyOfRange(items_current, starttime, 17), ","));
                    StartPoints.put(startpoint, times);
                    TotalPoints += 1;
                }
            }
//
        }
    }

    //generate synthetic data
    public static void generateTrack(int NumOfStudents) {
        System.out.println(TotalPoints);
        System.out.println(StartPoints.size());
        String startpair, startap, currentap, end, end_interval, next_item[] = {};
        int i, frequency = 0, f = 0, s = 0;
        int No = 0, end_int, next_start_int;
        ArrayList<String> next_items = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> entry : StartPoints.entrySet()) {
            s = entry.getValue().size();//number of start points in original data
            startap = entry.getKey();
            Random random_start = new Random();

            frequency = Math.round(NumOfStudents * s / TotalPoints);//number of start points in new data
//            System.out.println(startap + "," + s + "," + frequency);
            f = frequency;
            while (f > 0) {
                f--;
                No++;
                startpair = entry.getValue().get(random_start.nextInt(s));
                new_data.add(No + "," + startpair);
                end = startpair.split(",")[1];
                end_int = time2int(end, date);
                end_interval = time2interval(end_int);
                currentap = startap;
                while (Transition.contains(currentap, end_interval)) {
                    next_items = Transition.get(currentap, end_interval);
                    Collections.shuffle(next_items);
                    for (i = 0; i < 0.8 * next_items.size(); i++) {
                        next_item = next_items.get(i).split(",");
                        next_start_int = time2int(next_item[0], date);
                        if (next_start_int > end_int) break;
                    }
                    if (i < 0.8 * next_items.size()) {
                        new_data.add(No + "," + next_items.get(i));
                        end_int = time2int(next_item[1], date);
                        end_interval = time2interval(end_int);
                        currentap = next_item[aploc_map - starttime];
                    } else break;
                }
            }
        }
    }

    //output synthetic data
    public static void outputData(String filename) {
        File syn = new File(filename);
        try {
            BufferedWriter syn_writer = new BufferedWriter(new FileWriter(syn));
            syn_writer.write(",student_token,sessionstarttime,sessionendtime,day,bytesreceived,bytessent,eaptype," +
                    "apname,aplocation,aplocation_mapping,protocol,rssi,snr,ssid");
            syn_writer.newLine();
            System.out.println(new_data.size());
            for (int i = 0; i < new_data.size(); i++) {
                syn_writer.write(i+","+new_data.get(i));
                syn_writer.newLine();
            }
            syn_writer.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when outputing");
        } catch (IOException ex) {
            System.out.println("Error when read or write" + filename);
        }
    }

    //transform time(int) to interval(string)
    @Contract(pure = true)
    private static String time2interval(int end) {
        String interval_str = "";
//        System.out.println(end);
        if (end < 7 * 3600) interval_str = "-1-7";
        else if (end < 9 * 3600) interval_str = "7-9";
        else if (end < 11 * 3600) interval_str = "9-11";
        else if (end < 13 * 3600) interval_str = "11-13";
        else if (end < 15 * 3600) interval_str = "13-15";
        else if (end < 17 * 3600) interval_str = "15-17";
        else if (end < 19 * 3600) interval_str = "17-19";
        else if (end < 21 * 3600) interval_str = "19-21";
        else if (end < 23 * 3600) interval_str = "21-23";
        else interval_str = "23-7";
//        System.out.println(interval_str);
        return interval_str;
    }

    //transform time(string) to time(int)
    private static int time2int(@NotNull String timeString, @NotNull String day) {
        String hms[] = {}, ymd[] = {}, day_str;
        int time = 0;
        //System.out.println(day);
        day_str = day.split("-")[2];
        ymd = timeString.split(" ")[0].split("-");
        hms = timeString.split(" ")[1].split(":");
        time =
                Math.round(Integer.parseInt(hms[0]) * 3600 + Integer.parseInt(hms[1]) * 60 + Float.parseFloat(hms[2])) + (Integer.parseInt(ymd[2]) - Integer.parseInt(day_str)) * oneDay;
//        System.out.println(time);
        return time;
    }

    //delete the incomplete items in original data
    private static int check_data(@NotNull String items[]) {
        int flag = 0;

        int start_int, end_int;
        start_int = time2int(items[starttime], items[day_col]);
        end_int = time2int(items[endtime], items[day_col]);
        if (start_int > end_int) return 1;
        if (items[apName_ord].length() == 0) return 1;
        if (items[aploc].length() == 0) return 1;
        if (items.length != 17) return 1;
        return flag;
    }
}

//comparator for rank by student token
class Mycomparator_stu implements Comparator<String> {

    public int compare(@NotNull String s1, @NotNull String s2) {
        String items1[] = s1.split(",");
        String items2[] = s2.split(",");
        if (items1[1].equals(items2[1])) {
            return items1[4].compareTo(items2[4]);
        }
        return items1[1].compareTo(items2[1]);
    }

}