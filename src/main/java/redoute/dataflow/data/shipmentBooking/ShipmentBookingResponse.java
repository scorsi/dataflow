package redoute.dataflow.data.shipmentBooking;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * The Java-Object representation of the input XML
 *
 * @author sylvain corsini
 * @link http://prod.services.siege.red/Redoute/Shipment/TEMANDO/ShipmentBooking/1.0?wsdl
 */
@XmlRootElement(name = "GetShipmentBookingResponse_6.0", namespace = "http://Redoute/Shipment/TEMANDO/ShipmentBooking/1.0/GetShipmentBooking/6.0")
public class ShipmentBookingResponse implements Serializable {

    @XmlElement(name = "Response")
    public Response response;

    public String toJson() {
        return toJson(0, 4);
    }

    public String toJson(int level, int spaces_level) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("{\n");
        sb.append(response.toJson(level + 1, spaces_level));
        for (int i = 0; i < level * spaces_level; i++) {
            sb.append(" ");
        }
        sb.append("}\n");
        return sb.toString();
    }

}
