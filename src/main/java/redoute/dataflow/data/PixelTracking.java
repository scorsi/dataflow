package redoute.dataflow.data;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class PixelTracking implements Serializable {

    public enum Platform {
        Other,
        Web,
        Mobile
    }

    public enum PageType {
        Unknown,
        AccountPage,
        AddToCart,
        BasketPage,
        BrandPage,
        ErrorPage,
        HomePage,
        LandingPage,
        LoginPage,
        ProductDetailPage,
        ProductListPage,
        ReviewsPage,
        SearchResultPage,
        StaticPage
    }

    public enum BasketStep {
        FirstStep,
        DeliverySelectionStep,
        PaymentSelectionStep,
        Checkout
    }

    private PixelTracking(Map<String, String> params, PageType type) {
        this.type = type;
        this.attributes = new HashMap<>();

        userId = params.getOrDefault("user_id", null);
        String[] splitHost = params.get("host").toLowerCase().split("\\.");
        if (splitHost.length < 2) return;
        hostExtension = StringUtils.capitalize(splitHost[splitHost.length - 1]);
        switch (splitHost[0]) {
            case "www": { platform = Platform.Web; break; }
            case "m": { platform = Platform.Mobile; break; }
            default: { platform = Platform.Other; }
        }
        userAgent = params.get("user_agent");
        date = params.get("receive_timestamp");
    }

    public PageType type;

    public String userId;

    public String hostExtension;

    public Platform platform;

    public String date;

    public String userAgent;

    /**
     * Map containing attributes depending of the PixelTracking PageType
     */
    public HashMap<String, Object> attributes;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("\t\"type\": \"").append(type.name()).append("\",\n");
        sb.append("\t\"userId\": ").append(userId != null ? "\"" + userId + "\"" : null).append(",\n");
        sb.append("\t\"userAgent\": \"").append(userAgent).append("\",\n");
        sb.append("\t\"hostExtension\": \"").append(hostExtension).append("\",\n");
        sb.append("\t\"platform\": \"").append(platform.name()).append("\",\n");
        sb.append("\t\"date\": \"").append(date).append("\"\n");
        sb.append("\t\"attributes\": {\n");
        switch (type) {
            case ProductListPage: {
                sb.append("\t\t\"isLanding\": ").append((boolean) attributes.get("isLanding") ? "true" : "false").append(",\n");
                sb.append("\t\t\"categories\": [\n");
                String[] cats = (String[]) attributes.get("categories");
                for (int i = 0; i < cats.length; i++) {
                    sb.append("\t\t\t\"").append(cats[i]).append("\"");
                    if (i != cats.length - 1) sb.append(",");
                    sb.append("\n");
                }
                sb.append("\t\t]\n");
                break;
            }
            case ProductDetailPage: {
                sb.append("\t\t\"productId\": \"").append(attributes.get("productId")).append("\",\n");
//                sb.append("\t\t\"multiPdpId\": \"").append((boolean) attributes.get("multiPdpId") ? "true" : "false").append("\",\n");
                sb.append("\t\t\"multiProductIds\": [\n");
                String[] products = (String[]) attributes.get("multiProductIds");
                if (products != null)
                    for (int i = 0; i < products.length; i++) {
                        sb.append("\t\t\t\"").append(products[i]).append("\"");
                        if (i != products.length - 1) sb.append(",");
                        sb.append("\n");
                    }
                sb.append("\t\t]\n");
                break;
            }
            case SearchResultPage: {
                sb.append("\t\t\"keywords\": \"").append(attributes.get("keywords")).append("\",\n");
                sb.append("\t\t\"isSerp\": \"").append(attributes.get("isSerp")).append("\"\n");
                break;
            }
            case BasketPage: {
                BasketStep step = (BasketStep) attributes.get("step");
                sb.append("\t\t\"step\": \"").append(step.name()).append("\",\n");
                if (step == BasketStep.PaymentSelectionStep || step == BasketStep.Checkout)
                    sb.append("\t\t\"shippingMethod\": \"").append(attributes.get("shippingMethod")).append("\",\n");
                if (step == BasketStep.Checkout) {
                    sb.append("\t\t\"paymentMethod\": \"").append(attributes.get("paymentMethod")).append("\",\n");
                    sb.append("\t\t\"orderId\": \"").append(attributes.get("orderId")).append("\",\n");
                }
                sb.append("\t\t\"promoCode\": ").append(attributes.get("promoCode") != null ? "\"" + attributes.get("promoCode") + "\"" : null).append(",\n");
                sb.append("\t\t\"price\": ").append(attributes.get("price")).append(",\n");
                sb.append("\t\t\"orderedProductsNumber\": ").append(attributes.get("orderedProductsNumber")).append(",\n");
                sb.append("\t\t\"orderedProducts\": [\n");
                String[] products = (String[]) attributes.get("orderedProducts");
                for (int i = 0; i < products.length; i++) {
                    sb.append("\t\t\t\"").append(products[i]).append("\"");
                    if (i != products.length - 1) sb.append(",");
                    sb.append("\n");
                }
                sb.append("\t\t]\n");
                break;
            }
            case AddToCart: {
                sb.append("\t\t\"productId\": \"").append(attributes.get("productId")).append("\"\n");
                break;
            }
        }
        sb.append("\t}\n");
        sb.append("}");
        return sb.toString();
    }

    //
    // True constructors
    //

    public static PixelTracking create(Map<String, String> params) {
        if (!"www".equals(params.get("host").split("\\.")[0])) return null;
        try {
            switch (URLDecoder.decode(params.get("env_template").toLowerCase(), "UTF-8")) {
                case "plp": return createProductListPage(params);
                case "pdp": return createProductDetailPage(params);
//                case "hp": return createHomePage(params);
//                case "login": return createLoginPage(params);
                case "basket, step 1": return createBasketFirstStep(params);
                case "basket, delivery selection": return createBasketDeliveryStep(params);
                case "basket, payment selection": return createBasketPaymentStep(params);
                case "basket, checkout": return createBasketCheckout(params);
                case "add to cart": return createAddToCart(params);
                case "search": return createSearchResultPage(params);
//                case "serp": return createSearchEngineResultPage(params);
            }
        } catch (UnsupportedEncodingException e) { /* unused */ }
        return null;
    }

    private static PixelTracking createProductListPage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.ProductListPage);
        p.attributes.put("categories", params.get("page_breadcrumb_id").split("\\|"));
        p.attributes.put("isLanding", false);
        return p;
    }

    private static PixelTracking createProductDetailPage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params, PageType.ProductDetailPage);
        p.attributes.put("productId", params.get("product_id"));
        if (params.get("product_multi_ids") != null) {
            p.attributes.put("multiProductIds", params.get("product_multi_ids").split("\\|"));
//            p.attributes.put("multiPdpId", params.get("product_gamme"));
        } else {
            p.attributes.put("multiProductIds", null);
//            p.attributes.put("multiPdpId", null);
        }
        return p;
    }

    private static PixelTracking createLoginPage(Map<String, String> params) {
        return new PixelTracking(params,  PageType.LoginPage);
    }

    private static PixelTracking createHomePage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.HomePage);
        return p;
    }

    private static PixelTracking createBasketFirstStep(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params, PageType.BasketPage);
        p.attributes.put("step", BasketStep.FirstStep);
        if (params.get("order_product_id") == null) p.attributes.put("orderedProducts", new String[0]);
        else p.attributes.put("orderedProducts", params.get("order_product_id").split("\\|"));
        p.attributes.put("orderedProductsNumber", Integer.parseInt(params.get("order_products_number")));
        p.attributes.put("promoCode", params.get("order_promo_code"));
        p.attributes.put("price", params.get("order_amount_tf_without_sf"));
        return p;
    }

    private static PixelTracking createBasketDeliveryStep(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params, PageType.BasketPage);
        p.attributes.put("step", BasketStep.DeliverySelectionStep);
        p.attributes.put("orderedProducts", params.get("order_product_id").split("\\|"));
        p.attributes.put("orderedProductsNumber", Integer.parseInt(params.get("order_products_number")));
        p.attributes.put("promoCode", params.get("order_promo_code"));
        p.attributes.put("price", params.get("order_amount_tf_without_sf"));
        return p;
    }

    private static PixelTracking createBasketPaymentStep(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params, PageType.BasketPage);
        p.attributes.put("step", BasketStep.PaymentSelectionStep);
        p.attributes.put("orderedProducts", params.get("order_product_id").split("\\|"));
        p.attributes.put("orderedProductsNumber", Integer.parseInt(params.get("order_products_number")));
        p.attributes.put("promoCode", params.get("order_promo_code"));
        p.attributes.put("price", params.get("order_amount_tf_without_sf"));
        p.attributes.put("shippingMethod", params.get("order_shipping_method"));
        return p;
    }

    private static PixelTracking createBasketCheckout(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params, PageType.BasketPage);
        p.attributes.put("step", BasketStep.Checkout);
        p.attributes.put("orderedProducts", params.get("order_product_id").split("\\|"));
        p.attributes.put("orderedProductsNumber", Integer.parseInt(params.get("order_products_number")));
        p.attributes.put("promoCode", params.get("order_promo_code"));
        p.attributes.put("price", params.get("order_amount_tf_without_sf"));
        p.attributes.put("shippingMethod", params.get("order_shipping_method"));
        p.attributes.put("paymentMethod", params.get("order_payment_methods"));
        p.attributes.put("orderId", params.get("order_id"));
        return p;
    }

    private static PixelTracking createSearchEngineResultPage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.SearchResultPage);
        p.attributes.put("keywords", params.get("search_keywords"));
        p.attributes.put("isSerp", true);
        return p;
    }

    private static PixelTracking createSearchResultPage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.SearchResultPage);
        p.attributes.put("keywords", params.get("search_keywords"));
        p.attributes.put("isSerp", false);
        return p;
    }

    private static PixelTracking createErrorPage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.ErrorPage);
        return p;
    }

    private static PixelTracking createStaticPage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.StaticPage);
        return p;
    }

    private static PixelTracking createReviewsPage(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.ReviewsPage);
        return p;
    }

    private static PixelTracking createAddToCart(Map<String, String> params) {
        PixelTracking p = new PixelTracking(params,  PageType.AddToCart);
        p.attributes.put("productId", params.get("add_product_id"));
        return p;
    }

    private static PixelTracking createLandingPage(Map<String, String> params) {
        PixelTracking page = new PixelTracking(params, PageType.LandingPage);
//        String typePage = URLDecoder.decode(obj.getJSONObject("httpRequest").getString("referer").toLowerCase(), "UTF-8").split("(http://)|(https://)")[1].split("/")[1];
//        switch (typePage.toLowerCase()) {
//            case "pplp": { page = createProductListPage(params, obj); break; }
//            case "ppdp": { page = createProductDetailPage(params, obj); break; }
//            default: { page = new PixelTracking(params,  PageType.Unknown); break; }
//        }
//        page.isLandingPage = true;
        return page;
    }

    private static PixelTracking createDefaultPage(Map<String, String> params) {
        // TODO
        PixelTracking p = new PixelTracking(params,  PageType.LandingPage);
        return p;
    }

}
