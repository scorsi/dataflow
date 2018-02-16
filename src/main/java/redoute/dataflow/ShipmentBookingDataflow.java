package redoute.dataflow;

import com.google.api.client.util.ArrayMap;
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
import redoute.dataflow.data.shipmentBooking.*;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
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
                .apply("Read the XML files from Pub/Sub",
                        PubsubIO.readStrings()
                                .fromSubscription(options.getPubSubSubscription()))

                .apply("Apply fixed window",
                        Window.into(SlidingWindows.of(Duration.standardMinutes(1)).every(Duration.standardMinutes(1))))

                .apply("Transform XML nodes to Java-Object", ParDo.of(new DoFn<String, ShipmentBookingResponse>() {
                    @ProcessElement
                    public void processeElement(ProcessContext c) {
                        try {
                            JAXBContext jaxbContext = JAXBContext.newInstance(ShipmentBookingResponse.class);
                            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                            StringReader reader = new StringReader(c.element());

                            ShipmentBookingResponse response = (ShipmentBookingResponse) unmarshaller.unmarshal(reader);
                            c.output(response);
                        } catch (JAXBException e) {
                            e.printStackTrace();
                        }
                    }
                }));

        // Write to PubSub
        shipments
                .apply("Create JSON String object from Java-Object", ParDo.of(new DoFn<ShipmentBookingResponse, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element().toJson());
                    }
                }))
                .apply("Write to Pub/Sub", ParDo.of(new DoFn<String, Void>() {
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

                            tableRow.get("orders")
                                    .add(new TableRow()
                                            .set("OrderId", response.orderId)
                                            .set("CustomerId", response.customerId)
                                            .set("SortingFilter", response.sortingFilter)
                                            .set("CarrierName", response.carrierName)
                                            .set("CarrierServiceName", response.carrierServiceName));
                            for (ShipmentBooked shipment : response.shipmentsBooked) {
                                tableRow.get("shipments")
                                        .add(new TableRow()
                                                .set("OrderIdRef", response.orderId)
                                                .set("ShipmentId", shipment.data.shipmentId)
                                                .set("Type", shipment.data.type)
                                                .set("CarrierConsignmentNumber", shipment.data.carrierConsignmentNumber));
                                for (LabelType label : shipment.data.labels) {
                                    tableRow.get("labels")
                                            .add(new TableRow()
                                                    .set("ShipmentIdRef", shipment.data.shipmentId)
                                                    .set("Type", label.type)
                                                    .set("Value", label.value));
                                }
                                for (DeliveryDetail detail : shipment.deliveriesDetails) {
                                    tableRow.get("details")
                                            .add(new TableRow()
                                                    .set("ShipmentIdRef", shipment.data.shipmentId)
                                                    .set("LineNumber", detail.lineNumber)
                                                    .set("PackageNumber", detail.packageNumber)
                                                    .set("EAN", detail.ean)
                                                    .set("Quantity", detail.itemQuantity));
                                }
                            }
                            c.output(tableRow);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }));

        tablesRow
                .apply("Get order TableRow", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("orders")) // Should only have 1
                            c.output(r);
                    }
                }))
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to("datapipeline-redoute:ShipmentBooking.Orders")
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("OrderId").setType("STRING"),
                                new TableFieldSchema().setName("CustomerId").setType("STRING"),
                                new TableFieldSchema().setName("SortingFilter").setType("STRING"),
                                new TableFieldSchema().setName("CarrierName").setType("STRING"),
                                new TableFieldSchema().setName("CarrierServiceName").setType("STRING")
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
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to("datapipeline-redoute:ShipmentBooking.Shipments")
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("OrderIdRef").setType("STRING"),
                                new TableFieldSchema().setName("ShipmentId").setType("STRING"),
                                new TableFieldSchema().setName("Type").setType("STRING"),
                                new TableFieldSchema().setName("CarrierConsignmentNumber").setType("STRING")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        tablesRow
                .apply("Get Labels TableRow", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("labels"))
                            c.output(r);
                    }
                }))
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to("datapipeline-redoute:ShipmentBooking.Labels")
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("ShipmentIdRef").setType("STRING"),
                                new TableFieldSchema().setName("Type").setType("STRING"),
                                new TableFieldSchema().setName("Value").setType("STRING")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        tablesRow
                .apply("Get Details TableRow", ParDo.of(new DoFn<Map<String, List<TableRow>>, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (TableRow r : c.element().get("details"))
                            c.output(r);
                    }
                }))
                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to("datapipeline-redoute:ShipmentBooking.Details")
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("ShipmentIdRef").setType("STRING"),
                                new TableFieldSchema().setName("LineNumber").setType("INTEGER"),
                                new TableFieldSchema().setName("PackageNumber").setType("INTEGER"),
                                new TableFieldSchema().setName("EAN").setType("STRING"),
                                new TableFieldSchema().setName("Quantity").setType("INTEGER")
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
