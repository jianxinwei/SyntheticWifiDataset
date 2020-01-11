package generateWifi;

import java.io.*;
import java.util.*;
import java.util.Set;
import java.util.HashSet;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.Tables;

/**
 *
 * @author Jianxin Wei
 */
public class statistics {

    public static void main(String args[]) {
//        int t=statistics.time2long("20180813 09:05:00.5");
//        System.out.println(t);
        setTimeslot(600);
//        String[] fileList = {"D:\\NUS\\DataTweaking\\wifi\\test.csv"};
        String[] fileList = {"D:\\NUS\\DataTweaking\\wifi\\wifi_2018-08-13.csv"};
        Integer[] dataSize = {50000};
//        long startTime=System.currentTimeMillis();   //获取开始时间
        Collect_Generate(fileList, dataSize);
//        long endTime=System.currentTimeMillis(); //获取结束时间
//        System.out.println("running time： "+(endTime-startTime)+"ms");
    }

    private static Map<String, Map<Long, Integer>> apName_time_stu = new HashMap();
    private static Table<String, String, Integer> apName_startend_stu = HashBasedTable.create();
    private static Map<String, Map<Long, Double[]>> apName_time_bytes = new HashMap();
    private static Map<Long, Integer> Stat_duration = new HashMap();
    private static Map<String, Set<String>> apName_ip = new HashMap();
    private static Table<String, String, Set<String>> apName_stu_mac = HashBasedTable.create();
    private static Map<String, String> apName_loc = new HashMap<>();
    private static Map<String, Set<String>> apName_protocol_ssid = new HashMap<>();
    private static Table<String, Integer, Integer> apName_rssi = HashBasedTable.create();
    private static Table<String, Integer, Integer> apName_ssr = HashBasedTable.create();
    private static Map<String, Double[]> apName_rssissr_GaussianCurve = new HashMap<>();
    private static Map<String, ArrayList<Integer[]>> apName_rssi_ssr = new HashMap<>();

    private static long total = 0;
    private static Map<String, Integer> apName_items = new HashMap<>();

    private static Map<String, ArrayList<Long[]>> mac_time = new HashMap<>();
    private static Map<String, ArrayList<Long[]>> ip_time = new HashMap<>();

    private static final int oneDay = 24 * 3600;
    private static int Timeslot = 1200;
    private static String date = "2018/8/13";
    private static final int student_token = 1, ipaddress_token = 2, mac_token = 3, starttime = 4, endtime = 5, day_col
            = 6,
            bytesreceived_ord = 7,
            bytessent_ord = 8, apName_ord = 10, aploc = 11, aploc_map = 12, proto_col = 13, rssi_col = 14, ssr_col = 15,
            ssid_col = 16;
    ;

    public static void setTimeslot(int timeslot) {
        Timeslot = timeslot;
    }

    //collect the distribution in original data
    public static void Collect_Generate(@NotNull String[] fileList, Integer[] dataSize) {
        int f = 0, i = 0, j = 0, rssi, snr;
//        int student_token = 1, ipaddress_token = 2, mac_token = 3, starttime = 4, endtime = 5, day = 6,
//                bytesreceived = 7,
//                bytessent = 8,
//                apName = 10, aploc = 11, aploc_map = 12, proto_col = 13, rssi_col = 14, ssr_col = 15, ssid_col = 16;
        String filename = "", headline = "", rowString = "";
        String apName_str = "", stuID = "", IP = "", macID = "", loc_str = "", loc_map_str = "", protocol = "", ssid
                = "";

        long start_long, end_long, received_long, sent_long;
        int flag1 = 0;

        if (fileList.length == 0) return;
        for (f = 0; f < fileList.length; ++f) {
            filename = fileList[f];
            File inFile1 = new File(filename);
            System.out.println("processing: " + inFile1.getName());
            try {
                BufferedReader reader1 = new BufferedReader(new FileReader(inFile1));
                headline = reader1.readLine();
                String colName[] = headline.split(",");
                while ((rowString = reader1.readLine()) != null) {
                    String items[] = rowString.split(",");
                    flag1 = check_data(items);
                    if (flag1 != 0) continue;
//                    System.out.println(items[0]);
                    start_long = time2long(items[starttime], items[day_col]);
                    end_long = time2long(items[endtime], items[day_col]);
                    date = items[day_col];
                    stuID = items[student_token];
                    IP = items[ipaddress_token];
                    macID = items[mac_token];
                    apName_str = items[apName_ord];
                    loc_str = items[aploc];
                    loc_map_str = items[aploc_map];
                    received_long = Long.parseLong(items[bytesreceived_ord]);
                    sent_long = Long.parseLong(items[bytessent_ord]);
                    protocol = items[proto_col];
                    rssi = Integer.parseInt(items[rssi_col]);
                    snr = Integer.parseInt(items[ssr_col]);
                    ssid = items[ssid_col];
//                    System.out.println("ap=" + apName_str);

                    put_Duration(start_long, end_long);
                    put_apName_time_stu(apName_str, start_long, end_long);
                    put_apName_startend_stu(apName_str, start_long, end_long);
                    put_apName_time_bytes(apName_str, start_long, end_long, received_long, sent_long);
                    put_apName_stu_mac(apName_str, stuID, macID);
                    put_apName_ip(apName_str, IP);
                    put_apName_loc(apName_str, loc_str + "," + loc_map_str);
                    put_apName_protocol_ssid(apName_str, protocol + "," + ssid);
//                    put_apName_rssi(apName_str, rssi);
//                    put_apName_ssr(apName_str, snr);
                    put_apName_rssi_snr(apName_str, rssi, snr);
                }
                reader1.close();
            } catch (FileNotFoundException ex) {
                System.out.println("File:" + filename + " not found when collecting");
            } catch (IOException ex) {
                System.out.println("Error when read or write inputFile");
            }

            String newFileName = inFile1.getParent() + "\\" + "new_" + inFile1.getName();
            Generate(dataSize[f], newFileName);
            System.out.println("Successfully generated");
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
//        compute_rssissr_GaussianCurve();
//        write_Duration("D:\\NUS\\DataTweaking\\wifi\\output\\duration"+Timeslot/60+".csv");
//        write_apName_time_stu("D:\\NUS\\DataTweaking\\wifi\\output\\apName_time_stu.csv");
//        write_apName_startend_stu("D:\\NUS\\DataTweaking\\wifi\\output\\apName_startend_stu.csv");
//        write_apName_time_bytes("D:\\NUS\\DataTweaking\\wifi\\output\\apName_time_bytes.csv");
//        write_apName_stu_mac("D:\\NUS\\DataTweaking\\wifi\\output\\apName_stu_mac.csv");
//        write_apName_ip("D:\\NUS\\DataTweaking\\wifi\\output\\apName_ip.csv");
//        write_apName_loc("D:\\NUS\\DataTweaking\\wifi\\output\\apName_loc.csv");
//        write_apName_protocol_ssid("D:\\NUS\\DataTweaking\\wifi\\output\\apName_protocol_ssid.csv");
//        write_apName_rssi_ssr("D:\\NUS\\DataTweaking\\wifi\\output\\apName_rssi_ssr.csv");
    }

    //generate synthetic data
    public static void Generate(long datasize, String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
//        Set<Cell<String, String, Integer>> cells = apName_startend_stu.cellSet();
        Map<String, Integer> startend_stu_map = new HashMap<>();
        Set<String> apNames = apName_startend_stu.rowKeySet();
        Random random_sec = new Random(), random_num = new Random();
        String[] ymd;
        String[] line = new String[17];
        line[9] = "PEAP";
        String student_str = "", ipaddress_str = "", mac_str = "", sessionstarttime = "",
                sessionendtime =
                        "", day_str = "", bytesreceived = "", bytessent = "", eaptype = "PEAP", aplocation = "",
                aplocation_mapping = "", protocol = "",
                ssid = "", rssi_str = "", ssr_str = "";
        String startend = "", startclock = "", startday = "", endclock = "";
        long ordinal = 0, number;
        long[] bytes = {1, 1};
        int concur, startclock_int, endclock_int, sec, min, hour, rssi, snr;
        int n, i, j, k, temp1, temp2;

        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write(",student_token,ipaddress_token,mac_token,sessionstarttime,sessionendtime,day," +
                    "bytesreceived,bytessent,eaptype,apname,aplocation,aplocation_mapping,protocol,rssi,snr,ssid");
            writer1.newLine();
            for (String apName : apNames) {
                line[aploc - 1] = apName;
                line[aploc] = apName_loc.get(apName).split(",")[0];
                line[aploc_map] = apName_loc.get(apName).split(",")[1];
                startend_stu_map = apName_startend_stu.row(apName);
//                random_pair = new String[apName_items.get(apName)];
                List<String> random_list = new LinkedList<>();
                for (Map.Entry<String, Integer> entry1 : startend_stu_map.entrySet()) {
                    startend = entry1.getKey();
                    concur = entry1.getValue();
                    for (i = 0; i < 0 + concur; i++) {
//                        random_pair[i] = startend;
//                        pair += 1;
                        random_list.add(startend);
                    }
                }
                Collections.shuffle(random_list);

                number = Math.floorDiv(datasize * apName_items.get(apName), total);
                if (number == 0) number = random_num.nextInt(1);
                //generate whole items for apName
                for (n = 0; n < number; n++) {
                    line[0] = Long.toString(ordinal);
                    line[day_col] = date;
                    if (n == random_list.size()) {
                        n -= random_list.size();
                        number -= random_list.size();
                        Collections.shuffle(random_list);
                    }
                    startend = random_list.get(n);
                    //starttime
                    startclock = startend.split(",")[0];
                    endclock = startend.split(",")[1];

                    if (startclock.equals(endclock)) {
                        temp1 = random_sec.nextInt(1200);
                        temp2 = random_sec.nextInt(1200);
                        startclock_int = Integer.parseInt(startclock) + Math.min(temp1, temp2);
                        endclock_int = Integer.parseInt(endclock) + Math.max(temp1, temp2);
                    } else {
                        startclock_int = Integer.parseInt(startclock) + random_sec.nextInt(1200);
                        endclock_int = Integer.parseInt(endclock) + random_sec.nextInt(1200);
                    }
                    if (startclock_int>endclock_int){
                        int swap_temp = startclock_int;
                        startclock_int = endclock_int;
                        endclock_int = swap_temp;
                    }
                    if (startclock_int < 0) {
                        ymd = date.split("-");
//                        System.out.println(date);
                        startday = ymd[0] + "-" + ymd[1] + "-" + Integer.toString(Integer.parseInt(ymd[2]) - 1);
                        bytes = compute_bytes(apName, startclock_int, endclock_int);
                        startclock_int += oneDay;
                    } else {
                        startday = date;
                        bytes = compute_bytes(apName, startclock_int, endclock_int);
                    }
                    line[bytesreceived_ord] = Long.toString(bytes[0]);
                    line[bytessent_ord] = Long.toString(bytes[1]);

                    sec = startclock_int % 60;
                    min = (startclock_int / 60) % 60;
                    hour = startclock_int / 3600;
                    sessionstarttime =
                            startday + " " + Integer.toString(hour) + ":" + Integer.toString(min) + ":" + Integer.toString(sec);
                    line[starttime] = sessionstarttime;
//                    System.out.println(sessionstarttime);
                    //endtime
                    sec = endclock_int % 60;
                    min = (endclock_int / 60) % 60;
                    hour = endclock_int / 3600;
                    sessionendtime =
                            date + " " + Integer.toString(hour) + ":" + Integer.toString(min) + ":" + Integer.toString(sec);
                    line[endtime] = sessionendtime;
//                    System.out.println(sessionendtime);



                    //assign ip
                    Set<String> ips = apName_ip.get(apName);
                    for (String ip : ips) {
                        if (check_ip_time(ip, startclock_int, endclock_int)) {
                            line[ipaddress_token] = ip;
                            break;
                        }
                    }
                    if (line[ipaddress_token].isEmpty()) {
                        line[ipaddress_token] = "Need IP";
                        System.out.println(ordinal + "Need IP");
                    }

                    //assign stu, mac
                    Map<String, Set<String>> stus_macs = apName_stu_mac.row(apName);
                    Object[] stus = stus_macs.keySet().toArray();
                    int random_stu = random_num.nextInt(stus.length);
                    for (int s = random_stu; s < stus.length + random_stu; s++) {
                        student_str = stus[s % stus.length].toString();
//                        System.out.println(student_str);
                        Set<String> macs = apName_stu_mac.get(apName, student_str);
                        for (String mac : macs) {
                            if (check_mac_time(mac, startclock_int, endclock_int)) {
                                line[mac_token] = mac;
                                line[student_token] = student_str;
                                break;
                            }
                        }
                        if (!line[mac_token].isEmpty() && !line[student_token].isEmpty()) {
                            break;
                        }
                    }
//                    for (Map.Entry<String, Set<String>> stu_mac: stus_macs.entrySet()) {
//                        student_str = stu_mac.getKey();
//                        Set<String> macs = stu_mac.getValue();
//                        for (String mac: macs) {
//                            if (check_mac_time(mac, startclock_int, endclock_int)) {
//                                line[mac_token] = mac;
//                                line[student_token] = student_str;
//                                break;
//                            }
//                        }
//                        if (!line[mac_token].isEmpty() && !line[student_token].isEmpty()) {
//                            break;
//                        }
//                    }

                    //assign protocol, mac
                    Set<String> protocols = apName_protocol_ssid.get(apName);
                    for (String protocol_ssid : protocols) {
//                        protocol =
//                        ssid =
                        line[proto_col] = protocol_ssid.split(",")[0];
                        line[ssid_col] = protocol_ssid.split(",")[1];
                        break;
                    }

                    //assign rssi, snr
                    rssi = random_num.nextInt(apName_rssi_ssr.get(apName).size());
                    rssi_str = apName_rssi_ssr.get(apName).get(rssi)[0].toString();
                    line[rssi_col] = rssi_str;
                    ssr_str = apName_rssi_ssr.get(apName).get(rssi)[1].toString();
                    line[ssr_col] = ssr_str;

                    writer1.write(String.join(",", line));
                    writer1.newLine();
                    ordinal += 1;
                }

            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_startend_stu");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_startend_stu");
        }
    }

    private static void initMap_time_stu(String apName) {
        Map<Long, Integer> time_stu = new HashMap<Long, Integer>();
        apName_time_stu.put(apName, time_stu);
    }

    private static void initMap_time_bytes(String apName) {
        Map<Long, Double[]> time_bytes = new HashMap<Long, Double[]>();
//        Double bytes[]=new Double[2];
        apName_time_bytes.put(apName, time_bytes);
    }

    //transform time(string) to time(int)
    private static int time2long(@NotNull String timeString, @NotNull String day) {
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

    //collect distribution of apName-time-number of students
    private static void put_apName_time_stu(String apName, long start, long end) {
        if (!apName_time_stu.containsKey(apName)) {
            initMap_time_stu(apName);
        }
        long s = Math.floorDiv(start, Timeslot);
        long e = Math.floorDiv(end, Timeslot);
        long is = 0;
        Map<Long, Integer> time_stu = apName_time_stu.get(apName);
        for (long i = s; i <= e; i++) {
            is = i * Timeslot;
//            System.out.println(is);
            if (!time_stu.containsKey(is)) {
                time_stu.put(is, 1);
            } else {
                time_stu.put(is, time_stu.get(is) + 1);
//                System.out.println(time_stu.get(is));
            }
        }
    }

    //collect distribution of apName-time-bytes
    private static void put_apName_time_bytes(String apName, long start, long end, long received, long sent) {
        if (!apName_time_bytes.containsKey(apName)) {
            initMap_time_bytes(apName);
        }
        long s = Math.floorDiv(start, Timeslot);
        long e = Math.floorDiv(end, Timeslot);
        long is = 0;
        long duration = end - start;
        if (duration == 0) duration = 1;
        double mean_received = received / duration, mean_sent = sent / duration;
        Double bytes[] = {mean_received, mean_received, mean_sent, mean_sent}, by2[];
        Map<Long, Double[]> time_bytes = apName_time_bytes.get(apName);
        for (long i = s; i <= e; i++) {
            is = i * Timeslot;
//            System.out.println(is);
            if (!time_bytes.containsKey(is)) {
                time_bytes.put(is, bytes);
            } else {
                by2 = time_bytes.get(is);
                if (by2[0] > mean_received) by2[0] = mean_received;
                if (by2[1] < mean_received) by2[1] = mean_received;
                if (by2[2] > mean_sent) by2[2] = mean_sent;
                if (by2[3] < mean_sent) by2[3] = mean_sent;
                time_bytes.put(is, by2);
            }
        }
    }

    //collect distribution of connection duration
    private static void put_Duration(long start, long end) {
//        if(end<start){
//            System.out.println(String.valueOf(end)+","+String.valueOf(start));
//        }
        long dur = end - start;
        if (!Stat_duration.containsKey(dur)) {
            Stat_duration.put(dur, 1);
        } else {
            Stat_duration.put(dur, Stat_duration.get(dur) + 1);
        }
    }

    //collect distribution of apName-time-number of students
    private static void put_apName_startend_stu(String apName, long start, long end) {
        long s = Math.floorDiv(start, Timeslot) * Timeslot;
        long e = Math.floorDiv(end, Timeslot) * Timeslot;
        String pair = Long.toString(s) + "," + Long.toString(e);

        total = total + 1;
        if (!apName_startend_stu.contains(apName, pair)) {
            apName_startend_stu.put(apName, pair, 1);
        } else {
            apName_startend_stu.put(apName, pair, 1 + apName_startend_stu.get(apName, pair));
        }
        if (!apName_items.containsKey(apName)) {
            apName_items.put(apName, 1);
        } else {
            apName_items.put(apName, apName_items.get(apName) + 1);
        }
    }

    //collect relationship between student-mac
    private static void put_apName_stu_mac(String apName, String stu, String mac) {
        Set<String> macs = new HashSet<>();
        if (!apName_stu_mac.contains(apName, stu)) {
            macs.add(mac);
            apName_stu_mac.put(apName, stu, macs);
        } else {
            macs = apName_stu_mac.get(apName, stu);
            macs.add(mac);
            apName_stu_mac.put(apName, stu, macs);
        }
    }

    //collect relationship between apName-ip address
    private static void put_apName_ip(String apName, String ip) {
        Set<String> IPs = new HashSet<>();
        if (!apName_ip.containsKey(apName)) {
            IPs.add(ip);
            apName_ip.put(apName, IPs);
        } else {
            IPs = apName_ip.get(apName);
            IPs.add(ip);
            apName_ip.put(apName, IPs);
        }
    }

    //collect relationship between apName-ap location
    private static void put_apName_loc(String apName, String loc) {
//        if (!apName_loc.containsKey(apName)) {
//            apName_loc.put(apName, loc);
//        } else {
//            apName_loc.put(apName, apName_loc.get(apName)+"→"+loc);
//        }
        apName_loc.put(apName, loc);
    }

    //collect distribution of apName-protocol-ssid
    private static void put_apName_protocol_ssid(String apName, String protocol_ssid) {
        Set<String> ptc_ssid = new HashSet<>();
        if (!apName_protocol_ssid.containsKey(apName)) {
            ptc_ssid.add(protocol_ssid);
            apName_protocol_ssid.put(apName, ptc_ssid);
        } else {
            ptc_ssid = apName_protocol_ssid.get(apName);
            ptc_ssid.add(protocol_ssid);
            apName_protocol_ssid.put(apName, ptc_ssid);
        }
//        System.out.println("apName =" + apName + " value = " + protocol_ssid);
    }

    //collect distribution of apName-rssi-snr
    private static void put_apName_rssi_snr(String apName, int rssi, int snr) {
        ArrayList<Integer[]> rssi_ssr_list = new ArrayList<>();
        Integer[] rssi_ssr = {rssi, snr};
        if (!apName_rssi_ssr.containsKey(apName)) {
            rssi_ssr_list.add(rssi_ssr);
            apName_rssi_ssr.put(apName, rssi_ssr_list);
        } else {
            rssi_ssr_list = apName_rssi_ssr.get(apName);
            rssi_ssr_list.add(rssi_ssr);
            apName_rssi_ssr.put(apName, rssi_ssr_list);
        }
    }

    //collect distribution of apName-rssi
    private static void put_apName_rssi(String apName, int rssi) {
        if (!apName_rssi.contains(apName, rssi)) {
            apName_rssi.put(apName, rssi, 1);
        } else {
            apName_rssi.put(apName, rssi, apName_rssi.get(apName, rssi) + 1);
        }
    }

    //collect distribution of apName-snr
    private static void put_apName_ssr(String apName, int snr) {
        if (!apName_ssr.contains(apName, snr)) {
            apName_ssr.put(apName, snr, 1);
        } else {
            apName_ssr.put(apName, snr, apName_ssr.get(apName, snr) + 1);
        }
    }

    //output the distribution of apName-protocol-ssid
    public static void write_apName_protocol_ssid(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        String apName = "", protocol_ssid = "";
        Set<String> ptc_ssid = new HashSet<>();
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,protocol,ssid");
            writer1.newLine();
            for (Map.Entry<String, Set<String>> entry : apName_protocol_ssid.entrySet()) {
                apName = entry.getKey();
                ptc_ssid = entry.getValue();
                Iterator<String> iterator = ptc_ssid.iterator();
                while (iterator.hasNext()) {
                    protocol_ssid = iterator.next();
                    writer1.write(apName + "," + protocol_ssid);
                    writer1.newLine();
                    System.out.println("apName =" + apName + " value = " + protocol_ssid);
                }
            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_protocol_ssid");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_protocol_ssid");
        }
    }

    public static void write_apName_loc(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        String apName = "", loc = "";

        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,loc");
            writer1.newLine();
            for (Map.Entry<String, String> entry : apName_loc.entrySet()) {
                apName = entry.getKey();
                loc = entry.getValue();
                writer1.write(apName + "," + loc);
                writer1.newLine();
            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_loc");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_loc");
        }
    }

    public static void write_apName_ip(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        String apName = "", ip = "";
        Set<String> IPs = new HashSet<>();

        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,ipaddress_token");
            writer1.newLine();
            for (Map.Entry<String, Set<String>> entry : apName_ip.entrySet()) {
                apName = entry.getKey();
                IPs = entry.getValue();
                Iterator<String> iterator = IPs.iterator();
                while (iterator.hasNext()) {
                    ip = iterator.next();
                    writer1.write(apName + "," + ip);
                    writer1.newLine();
                }
//              System.out.println("key =" + key + " value = " + value);
            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_ip");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_ip");
        }
    }

    public static void write_apName_stu_mac(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        Set<Cell<String, String, Set<String>>> cells = apName_stu_mac.cellSet();
        String apName = "", stu_token = "", mac_token = "";
        Set<String> macs = new HashSet<>();
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,student_token,mac_token");
            writer1.newLine();
            for (Cell<String, String, Set<String>> temp : cells) {
                apName = temp.getRowKey();
                stu_token = temp.getColumnKey();
                macs = temp.getValue();
                Iterator<String> iterator = macs.iterator();
                while (iterator.hasNext()) {
                    mac_token = iterator.next();
                    writer1.write(apName + "," + stu_token + "," + mac_token);
                    writer1.newLine();
//                    System.out.println(apName + "-" + stu_token + "-" + mac_token);
                }
            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_stu_mac");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_stu_mac");
        }
    }

    public static void write_Duration(String filename) {
        File writerD = new File(filename);
        File outDir = new File(writerD.getParent());
//        System.out.println(writerD.getParent());
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writerD));
            writer1.write("Duration,stu_num");
            writer1.newLine();
            for (Map.Entry<Long, Integer> entry : Stat_duration.entrySet()) {
//                double minutes =entry.getKey()/60;
//                String key = Double.toString(minutes);
                long key_int = entry.getKey();
                if (key_int >= 0) {
                    String key = String.valueOf(key_int);
                    String value = entry.getValue().toString();
                    writer1.write(key + "," + value);
                    writer1.newLine();
                }
//              System.out.println("key =" + key + " value = " + value);
            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output Duration");
        } catch (IOException ex) {
            System.out.println("Error when read or write Duration");
        }
    }

    public static void write_apName_time_stu(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        String apName = "", interval = "", num_str = "";
//        System.out.println(writerD.getParent());
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,starttime,endtime,NumOfStudents");
            writer1.newLine();
            for (Map.Entry<String, Map<Long, Integer>> entry1 : apName_time_stu.entrySet()) {
                apName = entry1.getKey();
                Map<Long, Integer> time_stu = apName_time_stu.get(apName);
                for (Map.Entry<Long, Integer> entry2 : time_stu.entrySet()) {
                    long start = entry2.getKey();
//                    if (start < 0) continue;
//                    interval = timekey2interval(start);
                    num_str = Integer.toString(time_stu.get(start));
                    writer1.write(apName + "," + Long.toString(start) + "," + num_str);
                    writer1.newLine();
//                    System.out.println("apName=" + apName + " key =" + interval + " value = " + num_str);
                }

            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_time_stu");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_time_stu");
        }
    }

    public static void write_apName_startend_stu(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        Set<Cell<String, String, Integer>> cells = apName_startend_stu.cellSet();
        String apName = "", startend = "", startclock = "", endclock = "";
        int number, min, hour;
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,start,end,number");
            writer1.newLine();
            for (Cell<String, String, Integer> temp : cells) {
                apName = temp.getRowKey();
                startend = temp.getColumnKey();
                startclock = startend.split(",")[0];
                min = (Integer.parseInt(startclock) / 60) % 60;
                hour = Integer.parseInt(startclock) / 3600;
                startclock = Integer.toString(hour) + ":" + Integer.toString(min);
                endclock = startend.split(",")[1];
                min = (Integer.parseInt(endclock) / 60) % 60;
                hour = Integer.parseInt(endclock) / 3600;
                endclock = Integer.toString(hour) + ":" + Integer.toString(min);
                number = temp.getValue();

                writer1.write(apName + "," + startclock + "," + endclock + "," + number);
                writer1.newLine();
            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_startend_stu");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_startend_stu");
        }
    }

    public static void write_apName_time_bytes(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        String apName = "", interval = "", bytesreceived = "", bytessent = "";
        Double[] bytes = new Double[4];
        Map<Long, Double[]> new_time_bytes = new HashMap<>();
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,starttime,endtime,bytesreceived(B/s),bytessent(B/s)");
            writer1.newLine();
            for (Map.Entry<String, Map<Long, Double[]>> entry1 : apName_time_bytes.entrySet()) {
                apName = entry1.getKey();
                Map<Long, Double[]> time_bytes = apName_time_bytes.get(apName);
                for (Map.Entry<Long, Double[]> entry2 : time_bytes.entrySet()) {
                    long start = entry2.getKey();
//                    System.out.println(apName_time_bytes.get(apName).get(start)[0]);
//                    System.out.println(apName_time_stu.get(apName).get(start));
//                    System.out.println(apName+","+start);
//                    bytes[0] =
//                            apName_time_bytes.get(apName).get(start)[0] / apName_time_stu.get(apName).get(start);
//                    bytes[1] =
//                            apName_time_bytes.get(apName).get(start)[1] / apName_time_stu.get(apName).get(start);
//                    time_bytes.put(start, bytes);
                    bytesreceived =
                            Double.toString(time_bytes.get(start)[0]) + "," + Double.toString(time_bytes.get(start)[1]);
                    bytessent =
                            Double.toString(time_bytes.get(start)[2]) + "," + Double.toString(time_bytes.get(start)[3]);
                    writer1.write(apName + "," + Long.toString(start) + "," + bytesreceived + "," + bytessent);
                    writer1.newLine();
//                    System.out.println("apName=" + apName + " key =" + interval + " received = " + bytesreceived +
//                    " " +
//                            "sent = " + bytessent);
                }

            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_time_bytes");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_time_bytes");
        }
    }

    public static void write_apName_rssi_ssr(String filename) {
        File writeFile = new File(filename);
        File outDir = new File(writeFile.getParent());
        String apName = "", rssi_ssr_str = "", rssi_str = "", ssr_str = "";

        ArrayList<Integer[]> rssi_ssr_list;
        if (!outDir.exists()) {
            outDir.mkdirs();
        }
        try {
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(writeFile));
            writer1.write("apName,rssi,snr");
            writer1.newLine();
            for (Map.Entry<String, ArrayList<Integer[]>> entry : apName_rssi_ssr.entrySet()) {
                apName = entry.getKey();
                rssi_ssr_list = entry.getValue();
                for (int r = 0; r < rssi_ssr_list.size(); r++) {
                    rssi_str = rssi_ssr_list.get(r)[0].toString();
                    ssr_str = rssi_ssr_list.get(r)[1].toString();
                    writer1.write(apName + "," + rssi_str + "," + ssr_str);
                    writer1.newLine();
//                    System.out.println("apName=" + apName + "," + rssi_str + "," + ssr_str);
                }
            }
            writer1.close();
        } catch (FileNotFoundException ex) {
            System.out.println("File:" + filename + " not found when output apName_time_bytes");
        } catch (IOException ex) {
            System.out.println("Error when read or write apName_time_bytes");
        }
    }

    //delete the incomplete items
    private static int check_data(@NotNull String items[]) {
        int flag = 0;
        int student_token = 1, mac_token = 3, starttime = 4, endtime = 5, day = 6, bytesreceived = 7, bytessent = 8,
                apName = 10, aploc = 11, aplocation_map = 12, rssi = 14, snr = 15, ssid = 16;
        long start_long, end_long;
        start_long = time2long(items[starttime], items[day]);
        end_long = time2long(items[endtime], items[day]);
        if (start_long > end_long) return 1;
        if (items[apName].length() == 0) return 1;
        if (items[aploc].length() == 0) return 1;
        if (items.length != 17) return 1;
        return flag;
    }

    //transform time to interval
    private static String timekey2interval(long cur_sec) {
        String interval = "";
        long cur_min, cur_h;
        if (cur_sec < 0) {
            cur_min = 0;
            cur_h = 0;
        } else {
            cur_min = (cur_sec / 60) % 60;
            cur_h = (cur_sec / 3600);
        }
//        long next_sec = cur_sec+Timeslot;
        long next_min = ((cur_sec + Timeslot) / 60) % 60, next_h = (cur_sec + Timeslot) / 3600;
        interval =
                Long.toString(cur_h) + ":" + Long.toString(cur_min) + "," + Long.toString(next_h) + ":" + Long.toString(next_min);
        return interval;
    }

    //compute the sent & received bytes
    private static long[] compute_bytes(String apName, long start, long end) {
        long s_interval = Math.floorDiv(start, Timeslot) * Timeslot;
        long e_interval = Math.floorDiv(end, Timeslot) * Timeslot;
        Double[] bytes_per_sec = new Double[4];
        Double[] speed = new Double[2];
        Random ran = new Random();
        long bytes_r = 0, bytes_s = 0;
        if (s_interval == e_interval) {
            bytes_per_sec = apName_time_bytes.get(apName).get(s_interval);
            if (bytes_per_sec == null) {
                speed[0] = 0.0;
                speed[1] = 0.0;
            } else {
//                System.out.println(apName + "," + s_interval + "," + bytes_per_sec[0]);
                speed[0] =
                        Math.random() * (bytes_per_sec[1] - bytes_per_sec[0] + 1) + bytes_per_sec[0] * (Math.random() * 0.5 + 0.5);
                speed[1] =
                        Math.random() * (bytes_per_sec[3] - bytes_per_sec[2] + 1) + bytes_per_sec[2] * (Math.random() * 0.5 + 0.5);
            }

            bytes_r = Math.round(speed[0] * (end - start));
            bytes_s = Math.round(speed[1] * (end - start));
        } else {
            for (long i = s_interval; i <= e_interval; i += Timeslot) {
                bytes_per_sec = apName_time_bytes.get(apName).get(i);
                if (bytes_per_sec == null) {
                    speed[0] = 0.0;
                    speed[1] = 0.0;
                } else {
//                    System.out.println(apName + "," + s_interval + "," + bytes_per_sec[0]);
                    speed[0] =
                            Math.random() * (bytes_per_sec[1] - bytes_per_sec[0] + 1) + bytes_per_sec[0] * (Math.random() * 0.5 + 0.5);
                    speed[1] =
                            Math.random() * (bytes_per_sec[3] - bytes_per_sec[2] + 1) + bytes_per_sec[2] * (Math.random() * 0.5 + 0.5);
                }

                bytes_r += Math.round(speed[0] * Timeslot);
                bytes_s += Math.round(speed[1] * Timeslot);
                if (i == s_interval) {
                    bytes_r -= Math.round(speed[0] * (start - s_interval));
                    bytes_s -= Math.round(speed[1] * (start - s_interval));
                }
                if (i == e_interval) {
                    bytes_r -= Math.round(speed[0] * (e_interval + Timeslot - end));
                    bytes_s -= Math.round(speed[1] * (e_interval + Timeslot - end));
                }
            }
        }
        long[] bytes = {bytes_r, bytes_s};
        return bytes;
    }

//    private static void compute_rssissr_GaussianCurve() {
//        WeightedObservedPoints rssi_obs = new WeightedObservedPoints();
//        WeightedObservedPoints ssr_obs = new WeightedObservedPoints();
//        double[] a = {0, 0};
//        Integer rssi, num;
//        Integer norm, mean, sigma;
//
//        for (String apName : apName_rssi.rowKeySet()) {
//            rssi_obs.clear();
//            for (Map.Entry<Integer, Integer> rssi_num : apName_rssi.row(apName).entrySet()) {
//                rssi = rssi_num.getKey();
//                num = rssi_num.getValue();
//                System.out.println("rssi:" + rssi + "num:" + num);
//                System.out.println();
//                rssi_obs.add(rssi, num);
//            }
//            List temp = rssi_obs.toList();
//            if (temp.size() < 3) {
//                continue;
//            }
//            System.out.println(temp.size());
//            double[] parameters = GaussianCurveFitter.create().fit(temp);
//            System.out.println("norm:" + parameters[0] + " mean:" + parameters[1] + " sigma:" + parameters[2]);
//        }
//    }

    //check whether the mac is connected to 2 AP at the same time
    @Contract(pure = true) //if not conflict, return true
    private static boolean check_mac_time(String mac, long start, long end) {
        boolean flag = true;
        Long[] time = {start, end};
        ArrayList<Long[]> using = new ArrayList<>();
        if (!mac_time.containsKey(mac)) {
            using.add(time);
            mac_time.put(mac, using);
            return true;
        }

        using = mac_time.get(mac);
        for (int u = 0; u < using.size(); u++) {
            if (start > using.get(u)[1] || end < using.get(u)[0]) {
                //success
                continue;
            } else {
                return false;
            }
        }
        using.add(time);
        mac_time.put(mac, using);
        return flag;
    }

    //check whether there are two identical ip address at the same time
    @Contract(pure = true) //if not conflict, return true
    private static boolean check_ip_time(String ip, long start, long end) {
        boolean flag = true;
        Long[] time = {start, end};
        ArrayList<Long[]> using = new ArrayList<>();
        if (!ip_time.containsKey(ip)) {
            using.add(time);
            ip_time.put(ip, using);
            return true;
        }

        using = ip_time.get(ip);
//        System.out.println(using.size());
        for (int u = 0; u < using.size(); u++) {
            if (start > using.get(u)[1] || end < using.get(u)[0]) {
                //success
                continue;
            } else {
                return false;
            }
        }
        using.add(time);
        ip_time.put(ip, using);
        return flag;
    }
}
