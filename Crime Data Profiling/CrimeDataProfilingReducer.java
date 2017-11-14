import java.io.IOException;
import java.lang.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeDataProfilingReducer extends Reducer<Text, Text, Text, Text> {
@Override
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                                String Type = "";
                                String Range = "";
                                String FinalValue = "";
                                int key1 = Integer.parseInt(key.toString());
                                String[] ColumnNames = {"Complaint_Num","Complaint_FromDate","Complaint_FromTime","Complaint_ToDate","Complaint_ToTime","Report_Date        ","Offense_KeyCode","Offense_Desc","Int_KeyCode","Internal_Desc","Crime_Status","Crime_Level","Juris_Desc","Boro_Name","Precinct","Spec_Location","Location_Desc","Park_Name","HousingDev_Name","X_Coord_NY","Y_Coord_NY"};

                                                switch(key1)     {
                                                        case 0:
                                                                        Type = "        BIGINT          ";
                                                                        Range = "Randomly Generated ID  ";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;
                                                        case 1:
                                                        case 3:
                                                        case 5:
                                                                        Type = "STRING          ";
                                                                        String minDate = "";
                                                                        String maxDate = "";
                                                                        ArrayList<String> dateList = new ArrayList<String>(); 
                                                                        for(Text value : values) {
                                                                        	String line1 = value.toString();
                                                                        	if(line1 != null && !line1.isEmpty()) {
                                                                        	int year = Integer.parseInt(line1.substring(6,line1.length()));
                                                                            int month = Integer.parseInt(line1.substring(0,2));
                                                                            int day = Integer.parseInt(line1.substring(3,5));
                                                                            String date = year + "/" + month + "/" + day;
                                                                            dateList.add(date);
                                                                            }
                                                                        }
                                                                        maxDate = Collections.min(dateList);
                                                                        minDate = Collections.max(dateList);
                                                                        Range = "[" + minDate + "  " + maxDate + "]";

                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;

                                                        case 2:
                                                        case 4:
                                                                        Type = "STRING          ";
                                                                        String maxTime = "";
                                                                        String minTime = "";
                                                                        ArrayList<String> timeList = new ArrayList<String>(); 
                                                                        for(Text value : values) {
                                                                            if(value.toString() != null && !value.toString().isEmpty()) {
                                                                        	   String line4 = value.toString();
                                                                        	   int hour = Integer.parseInt(line4.substring(0,2));
                                                                               int minute = Integer.parseInt(line4.substring(3,5));
                                                                               int sec = Integer.parseInt(line4.substring(6,line4.length()));
                                                                               String time = hour + ":" + minute + ":" + sec;
                                                                               timeList.add(time);
                                                                            }
                                                                        }
                                                                        maxTime = Collections.max(timeList);
                                                                        minTime = Collections.min(timeList);
                                                                        Range = "[" + minTime + "  " + maxTime + "]";
                                                                            
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;

                                                        case 6:
                                                        case 8:
                                                        case 14:
                                                                        Type = "        INT             ";
                                                                        int minValue = Integer.MAX_VALUE;
                                                                        int maxValue = Integer.MIN_VALUE;
                                                                        for(Text value : values) {
                                                                        		if(value.toString() != null && !value.toString().isEmpty()) {
                                                                                	maxValue = Math.max(maxValue, Integer.parseInt(value.toString()));
                                                                                	minValue = Math.min(minValue, Integer.parseInt(value.toString()));
                                                                           		}
                                                                        }

                                                                        Range = "[" + minValue + "  " + maxValue + "]";

                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;

                                                        case 7:
                                                                        Type = "        STRING          ";
                                                                        Range = "Description of offense corresponding with Offense_KeyCode";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;
                                                        case 9:
                                                                        Type = "        STRING          ";
                                                                        Range = "Description of internal classification corresponding with Internal_KeyCode";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;
                                                        case 10:
                                                        case 11:
                                                        case 12:
                                                        case 13:
                                                        case 15:
                                                                        Type = "        STRING          ";
                                                                        StringBuilder builder = new StringBuilder();
                                                                        ArrayList<String> wordList = new ArrayList<String>(); 
                                                                        for(Text value : values) {
                                                                        	if(value.toString() != null && !value.toString().isEmpty()) {
                                                                                if(wordList.isEmpty()) {
                                                                                    wordList.add(value.toString());
                                                                                }
                                                                                if(!wordList.contains(value.toString())) {
                                                                                                wordList.add(value.toString());
                                                                                } 
                                                                            }
                                                                        }
                                                                        for(String s : wordList) {
                                                                                builder.append(s);
                                                                                builder.append(", ");
                                                                        }
                                                                        Range = "{" + builder + "}";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;

                                                        case 16:
                                                                        Type = "        STRING          ";
                                                                        Range = "Specific Description of Premise";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;

                                                        case 17:
                                                                        Type = "        STRING          ";
                                                                        Range = "Name of Park/Playground/Greenspace if applicable";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;

                                                        case 18:
                                                                        Type = "        STRING          ";
                                                                        Range = "Name of Housing Development if applicable";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;

                                                        case 19:
                                                        case 20:
                                                                        Type = "        DECIMAL         ";
                                                                        double minCoord = Double.MAX_VALUE;
                                                                        double maxCoord = -Double.MAX_VALUE;
                                                                        for(Text value : values) {
                                                                                if(value.toString() != null && !value.toString().isEmpty()) {
                                                                                    maxCoord = Math.max(maxCoord, Double.parseDouble(value.toString()));
                                                                                    minCoord = Math.min(minCoord, Double.parseDouble(value.toString()));
                                                                                }
                                                                        }

                                                                        Range = "[" + minCoord + "  " + maxCoord + "]";
                                                                        FinalValue = Type.concat(Range);
                                                                        context.write(new Text(ColumnNames[key1]), new Text(FinalValue));
                                                                        break;
                                                        /*case 21:
 *                                                                      Type = "DECIMAL         ";
 *                                                                                                                              case 22:
 *                                                                                                                                                                                                      Type = "DECIMAL         ";*/
                                                }
                }
}