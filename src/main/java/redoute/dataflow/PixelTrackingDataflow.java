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
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redoute.dataflow.data.PixelTracking;

import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Map;

public class PixelTrackingDataflow {

    private static final Logger LOG = LoggerFactory.getLogger(PixelTrackingDataflow.class);

    public interface PixelTrackingDataflowOptions extends PipelineOptions {
        @Description("PubSub subscription")
        @Validation.Required
        ValueProvider<String> getPubsubInput();
        void setPubsubInput(ValueProvider<String> value);

        @Description("BigQuery Table for Addtocart data")
//        @Validation.Required
        ValueProvider<String> getAddtocartBigQueryOutput();
        void setAddtocartBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Basket data")
        @Validation.Required
        ValueProvider<String> getBasketBigQueryOutput();
        void setBasketBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Productlist data")
        @Validation.Required
        ValueProvider<String> getProductlistBigQueryOutput();
        void setProductlistBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Productdetails data")
        @Validation.Required
        ValueProvider<String> getProductdetailsBigQueryOutput();
        void setProductdetailsBigQueryOutput(ValueProvider<String> value);

        @Description("BigQuery Table for Search data")
        @Validation.Required
        ValueProvider<String> getSearchBigQueryOutput();
        void setSearchBigQueryOutput(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        PixelTrackingDataflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PixelTrackingDataflowOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<PixelTracking> pixelTrackingCollection = p.apply("Read from PubSub", PubsubIO.readStrings().fromSubscription(options.getPubsubInput()))

                // TODO: Change to 10 Minutes
                .apply("Apply fixed sliding window", Window.into(SlidingWindows.of(Duration.standardSeconds(10)).every(Duration.standardSeconds(10))))

                .apply("Parse data", ParDo.of(new DoFn<String, PixelTracking>() {
                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            JSONObject obj = new JSONObject(c.element());
                            String req = obj.getJSONObject("httpRequest").getString("requestUrl").toLowerCase();
                            int cutPos = req.indexOf("?");
                            if (cutPos > 0) {
                                String[] pureParams = req.substring(cutPos + 1).split("&");
                                Map<String, String> params = new ArrayMap<>();
                                for (String p : pureParams) {
                                    String[] data = p.split("=");
                                    String value = null;
                                    if (data.length > 1) value = URLDecoder.decode(data[1], "UTF-8");
                                    params.put(data[0], value);
                                }
                                params.put("user_agent", obj.getJSONObject("httpRequest").getString("userAgent"));
                                params.put("receive_timestamp", obj.getString("receiveTimestamp"));
                                PixelTracking page = PixelTracking.create(params);
                                if (page == null) throw new RuntimeException("Invalid data");
//                                System.out.println(obj.getJSONObject("httpRequest"));
//                                System.out.println(page);
                                c.output(page);
                            }
                        } catch (Exception e) {
                            LOG.error("Unexpected error while parsing input. File was <[ " + c.element() + " ]>", e);
//                            System.out.println("Unexpected error while parsing input. File was <[ " + c.element() + " ]>\n"); e.printStackTrace();
                        }
                    }
                }));

        pixelTrackingCollection
                .apply("Get Addtocart data", ParDo.of(new DoFn<PixelTracking, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        PixelTracking p = c.element();
                        if (p.type == PixelTracking.PageType.AddToCart) {
                            c.output(new TableRow()
                                    .set("USER_ID", p.userId)
                                    .set("USER_AGENT", p.userAgent)
                                    .set("HOST_EXTENSION", p.hostExtension)
                                    .set("PLATFORM", p.platform.name())
                                    .set("DATE", p.date)
                                    .set("PRODUCT_ID", p.attributes.get("productId"))
                            );
                        }
                    }
                }))
                .apply("Write Addtocart data to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getAddtocartBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("USER_ID").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("USER_AGENT").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("HOST_EXTENSION").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("PLATFORM").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("DATE").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("PRODUCT_ID").setType("STRING").setMode("REQUIRED")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pixelTrackingCollection
                .apply("Get Basket data", ParDo.of(new DoFn<PixelTracking, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        PixelTracking p = c.element();
                        if (p.type == PixelTracking.PageType.BasketPage) {
                            c.output(new TableRow()
                                    .set("USER_ID", p.userId)
                                    .set("USER_AGENT", p.userAgent)
                                    .set("HOST_EXTENSION", p.hostExtension)
                                    .set("PLATFORM", p.platform.name())
                                    .set("DATE", p.date)
                                    .set("STEP", ((PixelTracking.BasketStep) p.attributes.get("step")).name())
                                    .set("SHIPPING_METHOD", p.attributes.get("shippingMethod"))
                                    .set("PAYMENT_METHOD", p.attributes.get("paymentMethod"))
                                    .set("ORDER_ID", p.attributes.get("orderId"))
                                    .set("PROMO_CODE", p.attributes.get("promoCode"))
                                    .set("PRICE", p.attributes.get("price"))
                                    .set("PRODUCT_NUMBER", p.attributes.get("orderedProductsNumber"))
                                    .set("PRODUCT_IDS", p.attributes.get("orderedProducts") != null ? String.join("|", (String[]) p.attributes.get("orderedProducts")) : null)
                            );
                        }
                    }
                }))
                .apply("Write Basket data to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getBasketBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("USER_ID").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("USER_AGENT").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("HOST_EXTENSION").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("PLATFORM").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("DATE").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("STEP").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("SHIPPING_METHOD").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("PAYMENT_METHOD").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("ORDER_ID").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("PROMO_CODE").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("PRICE").setType("FLOAT").setMode("REQUIRED"),
                                new TableFieldSchema().setName("PRODUCT_NUMBER").setType("INTEGER").setMode("REQUIRED"),
                                new TableFieldSchema().setName("PRODUCT_IDS").setType("STRING").setMode("NULLABLE")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pixelTrackingCollection
                .apply("Get Productdetails data", ParDo.of(new DoFn<PixelTracking, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        PixelTracking p = c.element();
                        if (p.type == PixelTracking.PageType.ProductDetailPage) {
                            c.output(new TableRow()
                                    .set("USER_ID", p.userId)
                                    .set("USER_AGENT", p.userAgent)
                                    .set("HOST_EXTENSION", p.hostExtension)
                                    .set("PLATFORM", p.platform.name())
                                    .set("DATE", p.date)
                                    .set("PRODUCT_ID", p.attributes.get("productId"))
                                    .set("MULTI_PRODUCT_IDS", p.attributes.get("multiProductIds") != null ? String.join("|", (String[]) p.attributes.get("multiProductIds")) : null)
                            );
                        }
                    }
                }))
                .apply("Write Productdetails data to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getProductdetailsBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("USER_ID").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("USER_AGENT").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("HOST_EXTENSION").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("PLATFORM").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("DATE").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("PRODUCT_ID").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("MULTI_PRODUCT_IDS").setType("STRING").setMode("NULLABLE")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pixelTrackingCollection
                .apply("Get Productlist data", ParDo.of(new DoFn<PixelTracking, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        PixelTracking p = c.element();
                        if (p.type == PixelTracking.PageType.ProductListPage) {
                            String[] categories = (String[]) p.attributes.get("categories");
                            c.output(new TableRow()
                                    .set("USER_ID", p.userId)
                                    .set("USER_AGENT", p.userAgent)
                                    .set("HOST_EXTENSION", p.hostExtension)
                                    .set("PLATFORM", p.platform.name())
                                    .set("DATE", p.date)
                                    .set("IS_LANDING", p.attributes.get("isLanding"))
                                    .set("CATEGORY_1", categories[0])
                                    .set("CATEGORY_2", categories.length > 1 ? categories[1] : null)
                                    .set("CATEGORY_3", categories.length > 2 ? categories[2] : null)
                                    .set("CATEGORY_4", categories.length > 3 ? categories[3] : null)
                                    .set("CATEGORY_5", categories.length > 4 ? categories[4] : null)
                                    .set("CATEGORY_6", categories.length > 5 ? categories[5] : null)
                            );
                        }
                    }
                }))
                .apply("Write Productlist data to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getProductlistBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("USER_ID").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("USER_AGENT").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("HOST_EXTENSION").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("PLATFORM").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("DATE").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("IS_LANDING").setType("BOOLEAN").setMode("REQUIRED"),
                                new TableFieldSchema().setName("CATEGORY_1").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("CATEGORY_2").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("CATEGORY_3").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("CATEGORY_4").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("CATEGORY_5").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("CATEGORY_6").setType("STRING").setMode("NULLABLE")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pixelTrackingCollection
                .apply("Get Search data", ParDo.of(new DoFn<PixelTracking, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        PixelTracking p = c.element();
                        if (p.type == PixelTracking.PageType.SearchResultPage) {
                            c.output(new TableRow()
                                    .set("USER_ID", p.userId)
                                    .set("USER_AGENT", p.userAgent)
                                    .set("HOST_EXTENSION", p.hostExtension)
                                    .set("PLATFORM", p.platform.name())
                                    .set("DATE", p.date)
                                    .set("IS_SERP", p.attributes.get("isSerp"))
                                    .set("KEYWORDS", p.attributes.get("keywords"))
                            );
                        }
                    }
                }))
                .apply("Write Search data to BigQuery", BigQueryIO.writeTableRows()
                        .to(options.getSearchBigQueryOutput())
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("USER_ID").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("USER_AGENT").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("HOST_EXTENSION").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("PLATFORM").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("DATE").setType("STRING").setMode("REQUIRED"),
                                new TableFieldSchema().setName("IS_SERP").setType("BOOLEAN").setMode("REQUIRED"),
                                new TableFieldSchema().setName("KEYWORDS").setType("STRING").setMode("NULLABLE")
                        )))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run().waitUntilFinish();
    }

}
