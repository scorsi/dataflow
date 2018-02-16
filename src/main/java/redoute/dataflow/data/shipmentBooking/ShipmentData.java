package redoute.dataflow.data.shipmentBooking;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.util.List;

@XmlType(name = "Shipment")
public class ShipmentData implements Serializable {

    /**
     * Enum with the following value:
     * - shipment
     * - shipmentReturn
     */
    @XmlAttribute(name = "type")
    public String type;

    @XmlElement(name = "ShipmentId")
    public String shipmentId;

    @XmlElement(name = "CarrierConsignmentNumber")
    public String carrierConsignmentNumber;

    @XmlElementWrapper(name = "Labels")
    @XmlElement(name = "Label", type = LabelType.class)
    public List<LabelType> labels;

    public String toJson(int level, int spaces_level) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"type\" : \"").append(type).append("\",\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"shipmentId\" : \"").append(shipmentId).append("\",\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"carrierConsignmentNumber\" : \"").append(carrierConsignmentNumber).append("\",\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"labels\" : [\n");
        for (int i = 0; i < labels.size(); i++) {
            for (int j = 0; j < (level + 1) * spaces_level; j++) {
                sb.append(" ");
            }
            sb.append("{\n");
            sb.append(labels.get(i).toJson(level + 2, spaces_level));
            for (int j = 0; j < (level + 1) * spaces_level; j++) {
                sb.append(" ");
            }
            sb.append("}");
            if (i != labels.size() - 1) {
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
