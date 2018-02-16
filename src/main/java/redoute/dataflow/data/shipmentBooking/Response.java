package redoute.dataflow.data.shipmentBooking;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.util.List;

@XmlType(name = "Response")
public class Response implements Serializable {

    @XmlElement(name = "OrderId")
    public String orderId;

    @XmlElement(name = "CustomerId")
    public String customerId;

    @XmlElement(name = "SortingFilter")
    public String sortingFilter;

    @XmlElement(name = "CarrierName")
    public String carrierName;

    @XmlElement(name = "CarrierServiceName")
    public String carrierServiceName;

    @XmlElementWrapper(name = "ShipmentsBooked")
    @XmlElement(name = "ShipmentBooked", type = ShipmentBooked.class)
    public List<ShipmentBooked> shipmentsBooked;

    public String toJson(int level, int spaces_level) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("\"orderId\" : \"").append(orderId).append("\",\n");
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("\"customerId\" : \"").append(customerId).append("\",\n");
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("\"sortingFilter\" : \"").append(sortingFilter).append("\",\n");
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("\"carrierName\" : \"").append(carrierName).append("\",\n");
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("\"carrierServiceName\" : \"").append(carrierServiceName).append("\",\n");
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("\"shipmentsBooked\" : [").append("\n");
        for (int i = 0; i < shipmentsBooked.size(); i++) {
            for (int j = 0; j < (level + 1) * spaces_level; j++) {
                sb.append(" ");
            }
            sb.append("{\n");
            sb.append(shipmentsBooked.get(i).toJson(level + 2, spaces_level));
            for (int j = 0; j < (level + 1) * spaces_level; j++) {
                sb.append(" ");
            }
            sb.append("}");
            if (i != shipmentsBooked.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("]\n");
        return sb.toString();
    }

}

