package redoute.dataflow.data.shipmentManifest;

import java.io.Serializable;

public class ShipmentManifest implements Serializable {

    public String orderId;

    public String carrierConsignmentNumber;

    public String date;

    public String eventType;

    public String eventLabel;

    public String eventCode;

    public String toJson() {
        return "{\n" +
                "\t\"orderId\": \"" + orderId + "\",\n" +
                "\t\"carrierConsignmentNumber\": \"" + carrierConsignmentNumber + "\",\n" +
                "\t\"date\": \"" + date + "\",\n" +
                "\t\"eventType\": \"" + eventType + "\",\n" +
                "\t\"eventLabel\": \"" + eventLabel + "\",\n" +
                "\t\"eventCode\": \"" + eventCode + "\",\n" +
                "}";
    }

}
