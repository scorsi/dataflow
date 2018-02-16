package redoute.dataflow.data.shipmentBooking;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;

@XmlType(name = "DeliveryDetail")
public class DeliveryDetail implements Serializable {

    @XmlElement(name = "LineNumber")
    public Integer lineNumber;

    @XmlElement(name = "PackageNumber")
    public Integer packageNumber;

    @XmlElement(name = "EAN")
    public String ean;

    @XmlElement(name = "ItemQuantity")
    public Integer itemQuantity;

    public String toJson(int level, int spaces_level) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"lineNumber\" : ").append(lineNumber).append(",\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"packageNumber\" : ").append(packageNumber).append(",\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"ean\" : \"").append(ean).append("\",\n");
        for (int j = 0; j < level * spaces_level; j++) {
            sb.append(" ");
        }
        sb.append("\"itemQuatity\" : ").append(itemQuantity).append("\n");
        return sb.toString();
    }

}
