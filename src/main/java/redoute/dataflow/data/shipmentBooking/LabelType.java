package redoute.dataflow.data.shipmentBooking;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;

@XmlType(name = "Label")
public class LabelType implements Serializable {

    /**
     * Enum with the following value:
     * - shipment
     * - invoiceProFormat
     * - customsDeclaration
     * - CODTurnInPage
     */
    @XmlAttribute(name = "type")
    public String type;

    @XmlElement(name = "Label")
    public String value;

    public String toJson(int level, int spaces_level) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"type\" : \"").append(type).append("\",\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"value\" : \"").append(value).append("\"\n");
        return sb.toString();
    }

}
