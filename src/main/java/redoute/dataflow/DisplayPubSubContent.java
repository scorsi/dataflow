package redoute.dataflow;

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
import org.joda.time.Duration;
import org.json.JSONObject;

import java.util.Arrays;

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

                .apply("Parse JSON", ParDo.of(new DoFn<String, TableRow>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        JSONObject obj = new JSONObject(c.element());
                        String transactionAmount = null;
                        String transactionId = null;
                        if (!obj.isNull("transaction")) {
                            JSONObject transaction = obj.getJSONObject("transaction");
                            transactionAmount = transaction.isNull("amount") ? null : transaction.getBigDecimal("amount").toString();
                            transactionId = transaction.isNull("tx_id") ? null : transaction.getBigDecimal("tx_id").toString();
                        }
                        c.output(new TableRow()
                                .set("date", obj.getString("date"))
                                .set("referer", obj.isNull("referer") ? null : obj.getString("referer"))
                                .set("url", obj.getString("url"))
                                .set("sessionId", obj.getString("sessionId"))
                                .set("localUid", obj.isNull("localuid") ? null : obj.getString("localuid"))
                                .set("userAgent", obj.getString("userAgent"))
                                .set("type", obj.isNull("type") ? null : obj.getString("type"))
                                .set("transactionAmount", transactionAmount)
                                .set("transactionId", transactionId));
                    }
                }))

                .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                        .to("gl-hackathon:NAV.TEST")
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("date").setType("STRING"),
                                new TableFieldSchema().setName("referer").setType("STRING"),
                                new TableFieldSchema().setName("url").setType("STRING"),
                                new TableFieldSchema().setName("sessionId").setType("STRING"),
                                new TableFieldSchema().setName("localUid").setType("STRING"),
                                new TableFieldSchema().setName("userAgent").setType("STRING"),
                                new TableFieldSchema().setName("type").setType("STRING"),
                                new TableFieldSchema().setName("transactionAmount").setType("STRING"),
                                new TableFieldSchema().setName("transactionId").setType("STRING")
                        ))))
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
