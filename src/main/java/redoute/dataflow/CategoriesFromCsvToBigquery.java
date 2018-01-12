package redoute.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import redoute.dataflow.data.Category;

public class CategoriesFromCsvToBigquery {

    private interface CategoriesFromCsvToBigqueryOptions extends PipelineOptions {
        @Description("Path of the CSV file containing the categories")
        @Validation.Required
        ValueProvider<String> getInputFile();

        @SuppressWarnings("unused")
        void setInputFile(ValueProvider<String> value);

        @Description("Path of the categories BigQuery table")
        @Validation.Required
        String getBigQueryTable();

        @SuppressWarnings("unused")
        void setBigQueryTable(String value);
    }

    public static void main(String[] args) {
        CategoriesFromCsvToBigqueryOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CategoriesFromCsvToBigqueryOptions.class);

        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from(options.getInputFile()))

                .apply("Transform lines from CSV to Category Object",
                        ParDo.of(new DoFn<String, Category>() {
                            @SuppressWarnings("unused")
                            @ProcessElement
                            public void processElement(ProcessContext context) {
                                Category category = Category.fromCsv(context.element());
                                if (category != null)
                                    context.output(category);
                            }
                        }))

                .apply("Transform Category Object to TableRow",
                        ParDo.of(new DoFn<Category, TableRow>() {
                            @SuppressWarnings("unused")
                            @ProcessElement
                            public void processElement(ProcessContext context) {
                                Category category = context.element();
                                if (category != null) {
                                    TableRow row = category.toTableRow();
                                    if (row != null)
                                        context.output(row);
                                }
                            }
                        }))

                .apply("Write TableRow into BigQuery",
                        BigQueryIO.writeTableRows()
                                .to(options.getBigQueryTable())
                                .withSchema(Category.getTableSchema())
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                );

        PipelineResult result = p.run();
        result.waitUntilFinish();
    }

}
