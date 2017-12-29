package redoute.dataflow.data;

import com.google.api.services.bigquery.model.TableRow;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement
public class TrackingHistory implements Serializable {

    @SuppressWarnings("WeakerAccess")
    static final String TRACKING_STATUS_TABLE_NAME = "trackingStatus";
    @SuppressWarnings("WeakerAccess")
    static final String TRACKING_STATUS_OCCURRED_TABLE_NAME = "trackingStatusOccurred";
    @SuppressWarnings("WeakerAccess")
    static final String TRACKING_FURTHER_DETAILS_TABLE_NAME = "trackingFurtherDetails";
    @SuppressWarnings("WeakerAccess")
    static final String TRACKING_CODE_TABLE_NAME = "trackingCode";

    @SuppressWarnings("WeakerAccess")
    @XmlElement
    public String trackingStatus;

    @SuppressWarnings("WeakerAccess")
    @XmlElement
    public String trackingStatusOccurred;

    @SuppressWarnings("WeakerAccess")
    @XmlElement
    public String trackingFurtherDetails;

    @SuppressWarnings("WeakerAccess")
    @XmlElement
    public String trackingCode;

    public TableRow toTableRow() {
        return new TableRow()
                .set(TRACKING_STATUS_TABLE_NAME, trackingStatus)
                .set(TRACKING_STATUS_OCCURRED_TABLE_NAME, trackingStatusOccurred)
                .set(TRACKING_FURTHER_DETAILS_TABLE_NAME, trackingFurtherDetails)
                .set(TRACKING_CODE_TABLE_NAME, trackingCode);
    }

    @Override
    public String toString() {
        return "TrackingHistory(trackingStatus=" + trackingStatus +
                ", trackingStatusOccurred=" + trackingStatusOccurred +
                ", trackingFurtherDetails=" + trackingFurtherDetails +
                ", trackingCode=" + trackingCode + ")";
    }
}
