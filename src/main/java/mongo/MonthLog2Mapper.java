package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
@Log4j

public class MonthLog2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    List<String> months = null;

    public MonthLog2Mapper() {

        this.months = Arrays.asList("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");

        String ip = fields[0];
        String reqTime = "";

        if (fields[3].length() > 2) {
            reqTime = fields[3].substring(1);
        }

        String reqMethod = "";

        if (fields[5].length() > 2) {
            reqMethod = fields[5].substring(1);
        }

        String reqURI = fields[6];

        String reqMonth = "";

        String[] dtFields = fields[3].split("/");

        if (dtFields.length > 1) {
            reqMonth = dtFields[1];
        }

        log.info("ip : " + ip);
        log.info("reqTime : " + reqTime);
        log.info("reqMethod : " + reqMethod);
        log.info("reqURI : " + reqURI);
        log.info("reqMonth : " + reqMethod);

        AccessLogDTO pDTO = new AccessLogDTO();
        pDTO.setIp(ip);
        pDTO.setReqTime(reqTime);
        pDTO.setReqMethod(reqMethod);
        pDTO.setReqURI(reqURI);

        String json = new ObjectMapper().writeValueAsString(pDTO);

        if (months.contains(reqMonth)) {
            context.write(new Text(reqMonth), new Text(json));
        }


    }

}
