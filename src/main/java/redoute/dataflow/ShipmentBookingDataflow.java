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

    static public void main(String[] args) {
        /* Create the pipeline options */
        ShipmentBookingDataflowOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(ShipmentBookingDataflowOptions.class);

        /* Create the pipeline */
        Pipeline pipeline = Pipeline.create(options);

        /* Apply transformations to the pipeline */
        PCollection<ShipmentBookingResponse> shipments = pipeline
                .apply("Read the XML files from PubSub",
                        PubsubIO.readStrings()
                                .fromSubscription(options.getPubSubSubscription()))

                .apply("Apply fixed window",
                        Window.into(SlidingWindows.of(Duration.standardMinutes(10)).every(Duration.standardMinutes(10))))

                .apply("Transform XML nodes to Java-Object", ParDo.of(new DoFn<String, ShipmentBookingResponse>() {
                    @ProcessElement
                    public void processeElement(ProcessContext c) {
                        try {
                            JAXBContext jaxbContext = JAXBContext.newInstance(ShipmentBookingResponse.class);
                            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                            StringReader reader = new StringReader(c.element());

                            ShipmentBookingResponse shipmentBooking = (ShipmentBookingResponse) unmarshaller.unmarshal(reader);
                            if (shipmentBooking == null || shipmentBooking.response == null) return;
                            c.output(shipmentBooking);
                        } catch (Exception e) {
                            e.printStackTrace();
                            LOG.error("Unexpected error while parsing XML file.\nFile was: <[\n" + c.element() + "\n]>", e);
                        }
                    }
                }));

        // Write to PubSub

        shipments
                .apply("Create JSON-Object from Java-Object", ParDo.of(new DoFn<ShipmentBookingResponse, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element().toJson());
                    }
                }))
                .apply("Write to PubSub", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.element();
                    }
                }));

        // Write to BigQuery

        PCollection<Map<String, List<TableRow>>> tablesRow = shipments
                .apply("Create BigQuery TableRows from Java-Object", ParDo.of(new DoFn<ShipmentBookingResponse, Map<String, List<TableRow>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            Response response = c.element().response;

                            Map<String, List<TableRow>> tableRow = new ArrayMap<>();
                            tableRow.put("orders", new ArrayList<>());
                            tableRow.put("shipments", new ArrayList<>());
                            tableRow.put("labels", new ArrayList<>());
                            tableRow.put("details", new ArrayList<>());

                            tableRow.get("orders").add(new TableRow().set("OrderId", response.orderId).set("CustomerId", response.customerId).set("SortingFilter", response.sortingFilter).set("CarrierName", response.carrierName).set("CarrierServiceName", response.carrierServiceName).set("Date", new DateTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").parse(c.element().responseStatus.timestamp)).toString()));
                            for (ShipmentBooked shipment : response.shipmentsBooked) {
                                tableRow.get("shipments").add(new TableRow().set("OrderIdRef", response.orderId).set("ShipmentId", shipment.data.shipmentId).set("Type", shipment.data.type).set("CarrierConsignmentNumber", shipment.data.carrierConsignmentNumber));
                                for (LabelType label : shipment.data.labels) {
                                    tableRow.get("labels").add(new TableRow().set("ShipmentIdRef", shipment.data.shipmentId).set("Type", label.type));
                                }
                                for (DeliveryDetail detail : shipment.deliveriesDetails) {
                                    tableRow.get("details").add(new TableRow().set("ShipmentIdRef", shipment.data.shipmentId).set("LineNumber", detail.lineNumber).set("PackageNumber", detail.packageNumber).set("EAN", detail.ean).set("Quantity", detail.itemQuantity));
                                }
                            }
                            c.output(tableRow);
                        } catch (Exception e) {
                            LOG.error("Unknown error", e);
                        }
                    }
                }));

        tablesRow
                .apply("Get Order TableRow", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("orders")) // Should only have 1
                            c.output(r);
                    }
                }))
                .apply("Write to datapipeline-redoute:SHIPMENT_BOOKING.Orders", BigQueryIO.writeTableRows()
                        .to("datapipeline-redoute:SHIPMENT_BOOKING.Orders")
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

        tablesRow
                .apply("Get Shipments TableRow", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("shipments"))
                            c.output(r);
                    }
                }))
                .apply("Write to datapipeline-redoute:SHIPMENT_BOOKING.Shipments", BigQueryIO.writeTableRows()
                        .to("datapipeline-redoute:SHIPMENT_BOOKING.Shipments")
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("OrderIdRef").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("ShipmentId").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("Type").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("CarrierConsignmentNumber").setType("STRING").setMode("REQUIRED")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        tablesRow.apply("Get Labels TableRow", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                for (TableRow r : c.element().get("labels"))
                    c.output(r);
            }
        })).apply("Write to datapipeline-redoute:SHIPMENT_BOOKING.Labels", BigQueryIO.writeTableRows().to("datapipeline-redoute:SHIPMENT_BOOKING.Labels").withSchema(new TableSchema().setFields(Arrays.asList(new TableFieldSchema().setName("ShipmentIdRef").setType("STRING").setMode("REQUIRED"), new TableFieldSchema().setName("Type").setType("STRING").setMode("REQUIRED")))).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        tablesRow
                .apply("Get Details TableRow", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("details"))
                            c.output(r);
                    }
                }))
                .apply("Write to datapipeline-redoute:SHIPMENT_BOOKING.Details", BigQueryIO.writeTableRows()
                        .to("datapipeline-redoute:SHIPMENT_BOOKING.Details")
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

    public interface ShipmentBookingDataflowOptions extends PipelineOptions {
        @Description("Pub/Sub subscription")
        @Validation.Required
        ValueProvider<String> getPubSubSubscription();

        @SuppressWarnings("unused")
        void setPubSubSubscription(ValueProvider<String> value);
    }

}
