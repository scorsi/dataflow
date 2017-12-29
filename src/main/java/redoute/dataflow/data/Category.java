package redoute.dataflow.data;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

public class Category implements Serializable {

    @SuppressWarnings("WeakerAccess")
    public String productId;
    @SuppressWarnings("WeakerAccess")
    public String categoryId;
    @SuppressWarnings("WeakerAccess")
    public String arbo;
    @SuppressWarnings("WeakerAccess")
    public String docId;

    @SuppressWarnings("WeakerAccess")
    public Category(String productId, String categoryId, String arbo, String docId) {
        this.productId = productId;
        this.categoryId = categoryId;
        this.arbo = arbo;
        this.docId = docId;
    }

    public static Category fromCsv(String input) {
        return fromCsv(input, ';');
    }

    @SuppressWarnings("WeakerAccess")
    public static Category fromCsv(String input, Character delim) {
        String[] fields = input.split(Pattern.quote(delim.toString()));
        if (fields.length != 4) {
            return null;
        }

        return new Category(fields[0], fields[1], fields[2], fields[3]);
    }

    public static TableSchema getTableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("productId").setType("STRING"),
                new TableFieldSchema().setName("categoryId").setType("STRING"),
                new TableFieldSchema().setName("arbo").setType("STRING"),
                new TableFieldSchema().setName("docId").setType("STRING")
        ));
    }

    public TableRow toTableRow() {
        TableRow row = new TableRow();
        row.set("productId", productId);
        row.set("categoryId", categoryId);
        row.set("arbo", arbo);
        row.set("docId", docId);
        return row;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Category) {
            Category o = (Category) obj;
            return this.productId.equals(o.productId) &&
                    this.categoryId.equals(o.categoryId) &&
                    this.arbo.equals(o.arbo) &&
                    this.docId.equals(o.arbo);
        }
        return false;
    }
}
