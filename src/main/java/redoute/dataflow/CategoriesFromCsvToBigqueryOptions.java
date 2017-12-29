package redoute.dataflow;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface CategoriesFromCsvToBigqueryOptions extends PipelineOptions {
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
