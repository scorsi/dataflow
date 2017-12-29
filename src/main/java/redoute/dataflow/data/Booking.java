package redoute.dataflow.data;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@XmlRootElement
public class Booking implements Serializable {

    @SuppressWarnings("WeakerAccess")
    static final String BOOKING_ID_TABLE_NAME = "bookingId";
    @SuppressWarnings("WeakerAccess")
    static final String CONSIGNMENT_NUMBER_TABLE_NAME = "consignmentNumber";

    @SuppressWarnings("WeakerAccess")
    @XmlElement
    public String bookingId;

    @SuppressWarnings("WeakerAccess")
    @XmlElement
    public String consignmentNumber;

    @XmlElementWrapper(name = "trackingHistories")
    @XmlElement(name = "trackingHistory", type = TrackingHistory.class)
    public List<TrackingHistory> trackingHistories;

    public static TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName(BOOKING_ID_TABLE_NAME).setType("STRING"),
                new TableFieldSchema().setName(CONSIGNMENT_NUMBER_TABLE_NAME).setType("STRING"),
                new TableFieldSchema().setName(TrackingHistory.TRACKING_STATUS_TABLE_NAME).setType("STRING"),
                new TableFieldSchema().setName(TrackingHistory.TRACKING_STATUS_OCCURRED_TABLE_NAME).setType("STRING"),
                new TableFieldSchema().setName(TrackingHistory.TRACKING_FURTHER_DETAILS_TABLE_NAME).setType("STRING"),
                new TableFieldSchema().setName(TrackingHistory.TRACKING_CODE_TABLE_NAME).setType("STRING")
        ));
    }

    public TableRow toTableRow() {
        return new TableRow()
                .set(BOOKING_ID_TABLE_NAME, bookingId)
                .set(CONSIGNMENT_NUMBER_TABLE_NAME, consignmentNumber);
    }

    public TableRow addToTableRow(TableRow tr) {
        return tr.set(BOOKING_ID_TABLE_NAME, bookingId)
                .set(CONSIGNMENT_NUMBER_TABLE_NAME, consignmentNumber);
    }

    @Override
    public String toString() {
        return "Booking(bookingId=" + bookingId +
                ", consignmentNumber=" + consignmentNumber +
                ", trackingHistories=" + trackingHistories + ")";
    }
}
