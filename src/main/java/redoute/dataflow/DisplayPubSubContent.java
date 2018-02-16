package redoute.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class DisplayPubSubContent {

    public static void main(String[] args) {
        DisplayPubSubContentOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DisplayPubSubContentOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply("Read from PubSub",
                PubsubIO.readStrings()
                        .fromSubscription(options.getPubSubSubscription()))

                .apply("Apply fixed window",
                        Window.into(SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(10))))

                .apply("Display content", ParDo.of(new DoFn<String, String>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element());
                    }
                }))

//                .apply("Show host attribute", ParDo.of(new DoFn<String, String>() {
//                    @SuppressWarnings("unused")
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        JSONObject obj = new JSONObject(c.element());
//                        System.out.println(obj.getString("host"));
//                    }
//                }))
        ;

        p.run().waitUntilFinish();
    }

    public interface DisplayPubSubContentOptions extends PipelineOptions {
        @Description("Pub/Sub subscription")
        @Validation.Required
        ValueProvider<String> getPubSubSubscription();

        @SuppressWarnings("unused")
        void setPubSubSubscription(ValueProvider<String> value);
    }

}
