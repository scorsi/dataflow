package redoute.dataflow.data;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class Bookings {

    @XmlElement(name = "booking")
    public List<Booking> bookings = null;

}
