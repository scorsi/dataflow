package redoute.dataflow;

import com.google.api.client.util.ArrayMap;
import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redoute.dataflow.data.shipmentBooking.*;
import redoute.dataflow.data.shipmentManifest.ShipmentManifest;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The Dataflow which handle the ShipmentBooking stream.
 *
 * @author sylvain corsini
 */
public class ShipmentBookingDataflow {

    private static final Logger LOG = LoggerFactory.getLogger(ShipmentBookingDataflow.class);

    public interface ShipmentBookingDataflowOptions extends PipelineOptions {
        @Description("PubSub subscription")
        @Validation.Required
        ValueProvider<String> getPubsubInput();
        void setPubsubInput(ValueProvider<String> value);

        @Description("BigQuery Table for Manifest data")
//        @Validation.Required
        ValueProvider<String> getManifestBigQueryOutput();
        void setManifestBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Booking Order data")
        @Validation.Required
        ValueProvider<String> getBookingOrderBigQueryOutput();
        void setBookingOrderBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Booking Shipment data")
        @Validation.Required
        ValueProvider<String> getBookingShipmentBigQueryOutput();
        void setBookingShipmentBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Booking Detail data")
        @Validation.Required
        ValueProvider<String> getBookingDetailBigQueryOutput();
        void setBookingDetailBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Booking Label data")
        @Validation.Required
        ValueProvider<String> getBookingLabelBigQueryOutput();
        void setBookingLabelBigQueryOutput(ValueProvider<String> value);

        @Description("PubSub Topic for Manifest data")
//        @Validation.Required
        ValueProvider<String> getManifestPubsubTopicOutput();
        void setManifestPubsubTopicOutput(ValueProvider<String> value);

        @Description("PubSub Topic for Booking data")
//        @Validation.Required
        ValueProvider<String> getBookingPubsubTopicOutput();
        void setBookingPubsubTopicOutput(ValueProvider<String> value);
    }

    static public void main(String[] args) {
        /* Create the pipeline options */
        ShipmentBookingDataflowOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(ShipmentBookingDataflowOptions.class);

        /* Create the pipeline */
        Pipeline pipeline = Pipeline.create(options);

        /* Apply transformations to the pipeline */
        PCollection<String> lines = pipeline
                .apply("Read from PubSub",
                        PubsubIO.readStrings()
                                .fromSubscription(options.getPubsubInput()))
                .apply("Apply fixed sliding window",
                        Window.into(SlidingWindows.of(Duration.standardMinutes(2))
                                .every(Duration.standardMinutes(2))));

        /*
        PCollection<ShipmentManifest> manifests = lines
                .apply("Parse Shipment Manifest", ParDo.of(new DoFn<String, ShipmentManifest>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            String line = c.element();
                            if (line.indexOf(0) == '<') return; // TODO: Should throw
                            String[] params = line.split("\\|");
                            if (params.length != 6) return; // TODO: Should throw
                            ShipmentManifest manifest = new ShipmentManifest();
                            manifest.orderId = params[0];
                            manifest.carrierConsignmentNumber = params[1];
                            manifest.date = new DateTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").parse(params[3])).toString();
                            manifest.eventType = params[2].toLowerCase();
                            manifest.eventLabel = params[4];
                            manifest.eventCode = params[5];
                            c.output(manifest);
                        } catch (Exception e) { /* unused * / }
                    }
                }));

        manifests
                .apply("Create TableRow with Manifest", ParDo.of(new DoFn<ShipmentManifest, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            ShipmentManifest manifest = c.element();
                            c.output(new TableRow()
                                    .set("ORDER_ID", manifest.orderId)
                                    .set("CARRIER_CONSIGNMENT_NUMBER", manifest.carrierConsignmentNumber)
                                    .set("DATE", manifest.date)
                                    .set("EVENT_TYPE", manifest.eventType)
                                    .set("EVENT_LABEL", manifest.eventLabel)
                                    .set("EVENT_CODE", manifest.eventLabel)
                            );
                        } catch (Exception e) {
                            LOG.error("Unknown error", e);
                        }
                    }
                }))
                .apply("Write Manifest to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getManifestBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("ORDER_ID").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("CARRIER_CONSIGNMENT_NUMBER").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("EVENT_LABEL").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("EVENT_TYPE").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("EVENT_CODE").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("DATE").setType("STRING").setMode("REQUIRED")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        manifests
                .apply("Create JSON with Manifest", ParDo.of(new DoFn<ShipmentManifest, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element().toJson());
                    }
                }))
                .apply("Write Manifest to PubSub", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.element();
                    }
                }));
        */

        PCollection<ShipmentBookingResponse> bookings = lines
                .apply("Parse Shipment Booking", ParDo.of(new DoFn<String, ShipmentBookingResponse>() {
                    @ProcessElement
                    public void processeElement(ProcessContext c) {
                        try {
                            JAXBContext jaxbContext = JAXBContext.newInstance(ShipmentBookingResponse.class);
                            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                            StringReader reader = new StringReader(c.element());

                            ShipmentBookingResponse shipmentBooking = (ShipmentBookingResponse) unmarshaller.unmarshal(reader);
                            if (shipmentBooking == null || shipmentBooking.response == null) throw new RuntimeException("Invalid data");
                            shipmentBooking.responseStatus.date = new DateTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").parse(shipmentBooking.responseStatus.timestamp)).toString();
                            c.output(shipmentBooking);
                        } catch (Exception e) {
                            LOG.error("Unexpected error while parsing input. File was <[ " + c.element() + " ]>", e);
                        }
                    }
                }));

        bookings
                .apply("Create JSON with Booking", ParDo.of(new DoFn<ShipmentBookingResponse, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element().toJson());
                    }
                }))
                .apply("Write Booking to PubSub", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.element();
                    }
                }));

        PCollection<Map<String, List<TableRow>>> bookingsTableRows = bookings
                .apply("Create all TablesRow with Booking", ParDo.of(new DoFn<ShipmentBookingResponse, Map<String, List<TableRow>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            ResponseStatus responseStatus = c.element().responseStatus;
                            Response response = c.element().response;

                            Map<String, List<TableRow>> tableRow = new ArrayMap<>();
                            tableRow.put("orders", new ArrayList<>());
                            tableRow.put("shipments", new ArrayList<>());
                            tableRow.put("labels", new ArrayList<>());
                            tableRow.put("details", new ArrayList<>());

                            tableRow.get("orders")
                                    .add(new TableRow()
                                            .set("OrderId", response.orderId)
                                            .set("CustomerId", response.customerId)
                                            .set("SortingFilter", response.sortingFilter)
                                            .set("CarrierName", response.carrierName)
                                            .set("CarrierServiceName", response.carrierServiceName)
                                            .set("Date", responseStatus.date)
                            );
                            for (ShipmentBooked shipment : response.shipmentsBooked) {
                                tableRow.get("shipments")
                                        .add(new TableRow()
                                                .set("OrderIdRef", response.orderId)
                                                .set("ShipmentId", shipment.data.shipmentId)
                                                .set("Type", shipment.data.type)
                                                .set("CarrierConsignmentNumber", shipment.data.carrierConsignmentNumber)
                                );
                                for (LabelType label : shipment.data.labels) {
                                    tableRow.get("labels")
                                            .add(new TableRow()
                                                    .set("ShipmentIdRef", shipment.data.shipmentId)
                                                    .set("Type", label.type)
                                            );
                                }
                                for (DeliveryDetail detail : shipment.deliveriesDetails) {
                                    tableRow.get("details")
                                            .add(new TableRow()
                                                    .set("ShipmentIdRef", shipment.data.shipmentId)
                                                    .set("LineNumber", detail.lineNumber)
                                                    .set("PackageNumber", detail.packageNumber)
                                                    .set("EAN", detail.ean)
                                                    .set("Quantity", detail.itemQuantity)
                                    );
                                }
                            }
                            c.output(tableRow);
                        } catch (Exception e) {
                            LOG.error("Unknown error", e);
                        }
                    }
                }));

        bookingsTableRows
                .apply("Get Orders", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("orders")) // Should only have 1
                            c.output(r);
                    }
                }))
                .apply("Write Order to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBookingOrderBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("OrderId").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("CustomerId").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("SortingFilter").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("CarrierName").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("CarrierServiceName").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("Date").setType("STRING").setMode("REQUIRED")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        bookingsTableRows
                .apply("Get Shipments", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("shipments"))
                            c.output(r);
                    }
                }))
                .apply("Write Shipment to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBookingShipmentBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("OrderIdRef").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("ShipmentId").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("Type").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("CarrierConsignmentNumber").setType("STRING").setMode("REQUIRED")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        bookingsTableRows
                .apply("Get Labels", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("labels"))
                            c.output(r);
                    }
                }))
                .apply("Write Label to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBookingLabelBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("ShipmentIdRef").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("Type").setType("STRING").setMode("REQUIRED")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        bookingsTableRows
                .apply("Get Details", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("details"))
                            c.output(r);
                    }
                }))
                .apply("Write Detail to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBookingDetailBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("ShipmentIdRef").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("LineNumber").setType("INTEGER").setMode("REQUIRED"),
                                new TableFieldSchema().setName("PackageNumber").setType("INTEGER").setMode("REQUIRED"),
                                new TableFieldSchema().setName("EAN").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("Quantity").setType("INTEGER").setMode("REQUIRED")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        /* Run the pipeline */
        pipeline.run().waitUntilFinish();
    }

}
