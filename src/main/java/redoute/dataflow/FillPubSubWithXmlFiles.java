package redoute.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import redoute.dataflow.data.Booking;

public class FillPubSubWithXmlFiles {

    static public void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        while (true) {
            Pipeline p = Pipeline.create(options);

            p.apply("Read XML Files",
                    XmlIO.<Booking>read()
                            .from("gs://data_bucket_flow/trackingData-20171227T150412161Z.xml")
                            .withRootElement("bookings")
                            .withRecordElement("booking")
                            .withRecordClass(Booking.class))

                    .apply("Transform Booking to String", ParDo.of(new DoFn<Booking, String>() {
                        @SuppressWarnings("unused")
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(c.element().toString());
                        }
                    }))

                    .apply("Write to PubSub",
                            PubsubIO.writeStrings()
                                    .to("projects/xenon-sunspot-180314/topics/dataflow_test"));

            p.run().waitUntilFinish();
        }
    }

}
