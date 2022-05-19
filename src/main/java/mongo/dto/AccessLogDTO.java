package mongo.dto;

import lombok.Getter;
import lombok.Setter;
import org.apache.htrace.shaded.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Getter
@Setter
public class AccessLogDTO {

    private String ip;
    private String reqTime;
    private String reqMethod;
    private String reqURI;



}
