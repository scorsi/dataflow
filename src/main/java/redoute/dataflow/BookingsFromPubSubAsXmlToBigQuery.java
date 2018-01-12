package redoute.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import redoute.dataflow.data.Booking;
import redoute.dataflow.data.Bookings;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;

public class BookingsFromPubSubAsXmlToBigQuery {

    public static void main(String[] args) {
        BookingsFromPubSubAsXmlToBigQueryOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(BookingsFromPubSubAsXmlToBigQueryOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply("Read from PubSub",
                PubsubIO.readStrings()
                        .fromSubscription(options.getPubSubSubscription()))

                .apply("Apply fixed window",
                        Window.into(SlidingWindows.of(Duration.standardMinutes(1)).every(Duration.standardSeconds(30))))

                .apply("Transform XML input to Booking Object", ParDo.of(new DoFn<String, Booking>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            // Try for one element
                            JAXBContext jaxbContext = JAXBContext.newInstance(Booking.class);
                            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                            StringReader reader = new StringReader(c.element());

                            Booking b = (Booking) unmarshaller.unmarshal(reader);
                            c.output(b);
                        } catch (Exception e1) {
                            // Try for list of elements
                            try {
                                JAXBContext jaxbContext = JAXBContext.newInstance(Bookings.class);
                                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                                StringReader reader = new StringReader(c.element());

                                Bookings b = (Bookings) unmarshaller.unmarshal(reader);
                                for (int i = 0; i < b.bookings.size(); ++i) {
                                    c.output(b.bookings.get(i));
                                }
                            } catch (Exception e2) {
                                // nothing
                            }
                        }
                    }
                }))

                .apply("Transform Booking Object to TableRow", ParDo.of(new DoFn<Booking, TableRow>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Booking b = c.element();
                        for (int i = 0; i < b.trackingHistories.size(); ++i) {
                            TableRow tr = b.trackingHistories.get(i).toTableRow();
                            c.output(b.addToTableRow(tr));
                        }
                    }
                }))

                .apply("Write TableRow into BigQuery",
                        BigQueryIO.writeTableRows()
                                .to(options.getBigQueryTable())
                                .withSchema(Booking.getTableSchema())
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                );

        p.run().waitUntilFinish();
    }

    public interface BookingsFromPubSubAsXmlToBigQueryOptions extends PipelineOptions {
        @Description("Pub/Sub subscription")
        @Validation.Required
        @Default.String("projects/xenon-sunspot-180314/subscriptions/dataflow_test")
        ValueProvider<String> getPubSubSubscription();

        @SuppressWarnings("unused")
        void setPubSubSubscription(ValueProvider<String> value);

        @Description("Path of the bookings BigQuery table")
        @Validation.Required
        @Default.String("xenon-sunspot-180314:dataflow_test.Bookings")
        ValueProvider<String> getBigQueryTable();

        @SuppressWarnings("unused")
        void setBigQueryTable(ValueProvider<String> value);
    }

}
