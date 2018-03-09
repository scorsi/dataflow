package redoute.dataflow.data.shipmentBooking;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;

@XmlType(name = "ResponseStatus")
public class ResponseStatus implements Serializable {

    @XmlElement(name = "StatusCode")
    public String statusCode;

    @XmlElement(name = "Timestamp")
    public String timestamp;

    public String date;

    @XmlElement(name = "RequestId")
    public String requestId;

}
