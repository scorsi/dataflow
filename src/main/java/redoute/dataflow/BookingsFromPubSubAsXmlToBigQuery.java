package redoute.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import redoute.dataflow.data.Booking;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;

public class BookingsFromPubSubAsXmlToBigQuery {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply("Read from PubSub",
                PubsubIO.readStrings()
                        .fromSubscription("projects/xenon-sunspot-180314/subscriptions/dataflow_test"))

                .apply("Apply fixed window",
                        Window.into(SlidingWindows.of(Duration.standardMinutes(1)).every(Duration.standardSeconds(30))))

                .apply("Display content", ParDo.of(new DoFn<String, String>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("String: " + c.element());
                        c.output(c.element());
                    }
                }))

                .apply("Transform XML input to Booking Object", ParDo.of(new DoFn<String, Booking>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            JAXBContext jaxbContext = JAXBContext.newInstance(Booking.class);
                            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                            StringReader reader = new StringReader(c.element());

                            Booking b = (Booking) unmarshaller.unmarshal(reader);
                            c.output(b);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }))

                .apply("Display content", ParDo.of(new DoFn<Booking, String>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("Object: " + c.element().toString());
                    }
                }));

        p.run().waitUntilFinish();
    }

    private interface BookingsFromPubSubAsXmlToBigQueryOptions extends PipelineOptions {
        @Description("Pub/Sub subscription")
        @Validation.Required
        ValueProvider<String> getPubSubSubscription();

        @SuppressWarnings("unused")
        void setPubSubSubscription(ValueProvider<String> value);

        @Description("Path of the bookings BigQuery table")
        @Validation.Required
        ValueProvider<String> getBigQueryTable();

        @SuppressWarnings("unused")
        void setBigQueryTable(ValueProvider<String> value);
    }

}
