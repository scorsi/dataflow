package redoute.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import redoute.dataflow.data.Booking;

public class BookingsFromXmlToBigQuery {

    private interface BookingsFromXmlToBigQueryOptions extends PipelineOptions {
        @Description("Path of the XML file containing the bookings")
        @Validation.Required
        @Default.String("gs://data_bucket_flow/trackingData-20171227T150412161Z.xml")
        ValueProvider<String> getInputFile();

        @SuppressWarnings("unused")
        void setInputFile(ValueProvider<String> value);

        @Description("Path of the bookings BigQuery table")
        @Validation.Required
        @Default.String("xenon-sunspot-180314:dataflow_test.Bookings")
        ValueProvider<String> getBigQueryTable();

        @SuppressWarnings("unused")
        void setBigQueryTable(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        BookingsFromXmlToBigQueryOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(BookingsFromXmlToBigQueryOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply("Read XML nodes and transform them to Booking Object",
                XmlIO.<Booking>read()
                        .from(options.getInputFile().get())
                        .withRootElement("bookings")
                        .withRecordElement("booking")
                        .withRecordClass(Booking.class))

                .apply("Transform Booking Object to TableRow", ParDo.of(new DoFn<Booking, TableRow>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        Booking b = context.element();
                        for (int i = 0; i < b.trackingHistories.size(); ++i) {
                            TableRow tr = b.trackingHistories.get(i).toTableRow();
                            context.output(b.addToTableRow(tr));
                        }
                    }
                }))

                .apply("Write TableRow into BigQuery",
                        BigQueryIO.writeTableRows()
                                .to(options.getBigQueryTable().get())
                                .withSchema(Booking.getTableSchema())
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                );

        PipelineResult result = p.run();
        result.waitUntilFinish();
    }

}
