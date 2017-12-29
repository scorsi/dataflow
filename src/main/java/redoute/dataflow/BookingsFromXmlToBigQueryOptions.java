package redoute.dataflow;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface BookingsFromXmlToBigQueryOptions extends PipelineOptions {
    @Description("Path of the XML file containing the bookings")
    @Validation.Required
    ValueProvider<String> getInputFile();

    @SuppressWarnings("unused")
    void setInputFile(ValueProvider<String> value);

    @Description("Path of the bookings BigQuery table")
    @Validation.Required
    ValueProvider<String> getBigQueryTable();

    @SuppressWarnings("unused")
    void setBigQueryTable(ValueProvider<String> value);
}
