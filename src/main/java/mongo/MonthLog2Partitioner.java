package mongo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class MonthLog2Partitioner extends Partitioner<Text, Text> {

    Map<String, Integer> month = new HashMap<>();

    public MonthLog2Partitioner() {
        this.month.put("Jan",0);
        this.month.put("Feb",1);
        this.month.put("Mar",2);
        this.month.put("Apr",3);
        this.month.put("May",4);
        this.month.put("Jun",5);
        this.month.put("Jul",6);
        this.month.put("Aug",7);
        this.month.put("Sep",8);
        this.month.put("Oct",9);
        this.month.put("Nov",10);
        this.month.put("Dec",11);
    }

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {

        return month.get(key.toString());
    }
}
