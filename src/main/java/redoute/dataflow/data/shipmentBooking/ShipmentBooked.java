package redoute.dataflow.data.shipmentBooking;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.util.List;

@XmlType(name = "ShipmentBooked")
public class ShipmentBooked implements Serializable {

    @XmlElement(name = "Shipment")
    public ShipmentData data;

    @XmlElementWrapper(name = "DeliveriesDetails")
    @XmlElement(name = "DeliveryDetails", type = DeliveryDetail.class)
    public List<DeliveryDetail> deliveriesDetails;

    public String toJson(int level, int spaces_level) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"data\" : {\n");
        sb.append(data.toJson(level + 1, spaces_level));
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("},\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"deliveriesDetails\" : [\n");
        for (int i = 0; i < deliveriesDetails.size(); i++) {
            for (int j = 0; j < (level + 1) * spaces_level; j++) {
                sb.append(" ");
            }
            sb.append("{\n");
            sb.append(deliveriesDetails.get(i).toJson(level + 2, spaces_level));
            for (int j = 0; j < (level + 1) * spaces_level; j++) {
                sb.append(" ");
            }
            sb.append("}");
            if (i != deliveriesDetails.size() - 1) {
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
