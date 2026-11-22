import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import java.io.StringReader;

public class OrbitServer {
    private static final Path PAGE = Path.of("index.html").toAbsolutePath().normalize();
    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    private static final Map<String, CachedFeed> CACHE = new ConcurrentHashMap<>();
    private static final Map<String, CachedFlightFeed> FLIGHT_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, String> MAP_DATA_CACHE = new ConcurrentHashMap<>();
    private static volatile CachedWildfireFeed WILDFIRE_CACHE;
    private static final Map<String, String> MAP_DATA_URLS = Map.of(
        "/api/map/countries", "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson",
        "/api/map/cities", "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_populated_places_simple.geojson"
    );

    private record Article(String title, String url, String source, String publishedAt) {}
    private record CachedFeed(Instant cachedAt, List<Article> articles) {}
    private record CachedFlightFeed(Instant cachedAt, String json) {}
    private record CachedWildfireFeed(Instant cachedAt, String json) {}

    public static void main(String[] args) throws IOException {
        String host = System.getenv().getOrDefault("HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8000"));
        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.createContext("/", OrbitServer::handle);
        server.start();
        System.out.println("Orbit is serving on " + host + ":" + port);
    }

    private static void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/api/news")) {
                if (!exchange.getRequestMethod().equals("GET")) {
                    send(exchange, 405, "application/json; charset=utf-8", "{\"error\":\"Use GET.\"}");
                    return;
                }
                serveNews(exchange);
                return;
            }
            if (path.equals("/api/flights")) {
                serveFlights(exchange);
                return;
            }
            if (path.equals("/api/wildfires")) {
                serveWildfires(exchange);
                return;
            }
            if (MAP_DATA_URLS.containsKey(path)) {
                serveMapData(exchange, path);
                return;
            }
            if (!path.equals("/") && !path.equals("/index.html")) {
                send(exchange, 404, "text/plain; charset=utf-8", "Not found");
                return;
            }
            byte[] content = Files.readAllBytes(PAGE);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(200, content.length);
            exchange.getResponseBody().write(content);
        } catch (Exception error) {
            System.err.println("Orbit request failed: " + error.getMessage());
            send(exchange, 502, "application/json; charset=utf-8", "{\"error\":\"The news service is temporarily unavailable.\"}");
        } finally {
            exchange.close();
        }
    }

    private static void serveFlights(HttpExchange exchange) throws Exception {
        if (!exchange.getRequestMethod().equals("GET")) {
            send(exchange, 405, "application/json; charset=utf-8", "{\"error\":\"Use GET.\"}");
            return;
        }
        String rawQuery = exchange.getRequestURI().getRawQuery();
        double latitude;
        double longitude;
        try {
            latitude = Double.parseDouble(queryParameter(rawQuery, "lat"));
            longitude = Double.parseDouble(queryParameter(rawQuery, "lon"));
        } catch (NumberFormatException | NullPointerException error) {
            send(exchange, 400, "application/json; charset=utf-8", "{\"error\":\"Valid latitude and longitude are required.\"}");
            return;
        }
        if (!Double.isFinite(latitude) || !Double.isFinite(longitude) ||
            latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
            send(exchange, 400, "application/json; charset=utf-8", "{\"error\":\"Coordinates must be within ±90° latitude and ±180° longitude.\"}");
            return;
        }

        Instant now = Instant.now();
        FLIGHT_CACHE.entrySet().removeIf(entry ->
            Duration.between(entry.getValue().cachedAt(), now).toSeconds() >= 15);
        String cacheKey = String.format(java.util.Locale.ROOT, "%.3f,%.3f", latitude, longitude);
        CachedFlightFeed cached = FLIGHT_CACHE.get(cacheKey);
        if (cached != null) {
            send(exchange, 200, "application/json; charset=utf-8", cached.json());
            return;
        }

        String path = String.format(java.util.Locale.ROOT,
            "https://api.adsb.lol/v2/lat/%.5f/lon/%.5f/dist/44", latitude, longitude);
        HttpRequest request = HttpRequest.newBuilder(URI.create(path))
            .timeout(Duration.ofSeconds(15))
            .header("User-Agent", "OrbitLocationGlobe/1.0")
            .GET()
            .build();
        HttpResponse<String> response;
        try {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException error) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The live aircraft feed could not be reached.\"}");
            return;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The live aircraft request was interrupted.\"}");
            return;
        }
        if (response.statusCode() != 200) {
            int status = response.statusCode() == 429 ? 429 : 502;
            send(exchange, status, "application/json; charset=utf-8",
                "{\"error\":\"The live aircraft feed returned HTTP " + response.statusCode() + ".\"}");
            return;
        }
        String data = response.body();
        if (!data.startsWith("{") || !data.contains("\"ac\"")) {
            send(exchange, 502, "application/json; charset=utf-8", "{\"error\":\"The live aircraft feed returned invalid data.\"}");
            return;
        }
        if (FLIGHT_CACHE.size() >= 500) {
            FLIGHT_CACHE.entrySet().stream()
                .min(Map.Entry.comparingByValue(java.util.Comparator.comparing(CachedFlightFeed::cachedAt)))
                .ifPresent(oldest -> FLIGHT_CACHE.remove(oldest.getKey(), oldest.getValue()));
        }
        FLIGHT_CACHE.put(cacheKey, new CachedFlightFeed(Instant.now(), data));
        send(exchange, 200, "application/json; charset=utf-8", data);
    }

    private static void serveWildfires(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            send(exchange, 405, "application/json; charset=utf-8", "{\"error\":\"Use GET.\"}");
            return;
        }
        Instant now = Instant.now();
        CachedWildfireFeed cached = WILDFIRE_CACHE;
        if (cached != null && Duration.between(cached.cachedAt(), now).toMinutes() < 15) {
            send(exchange, 200, "application/json; charset=utf-8", cached.json());
            return;
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(
                "https://eonet.gsfc.nasa.gov/api/v3/events?category=wildfires&status=all&days=30&limit=1000"))
            .timeout(Duration.ofSeconds(20))
            .header("User-Agent", "BetterEarthWildfireMap/1.0")
            .GET()
            .build();
        HttpResponse<String> response;
        try {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException error) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"NASA EONET wildfire data could not be reached.\"}");
            return;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The NASA EONET wildfire request was interrupted.\"}");
            return;
        }
        if (response.statusCode() != 200) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"NASA EONET returned HTTP " + response.statusCode() + ".\"}");
            return;
        }
        String data = response.body();
        if (!data.startsWith("{") || !data.contains("\"events\"")) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"NASA EONET returned invalid wildfire data.\"}");
            return;
        }
        WILDFIRE_CACHE = new CachedWildfireFeed(Instant.now(), data);
        send(exchange, 200, "application/json; charset=utf-8", data);
    }

    private static void serveMapData(HttpExchange exchange, String path) throws Exception {
        if (!exchange.getRequestMethod().equals("GET")) {
            send(exchange, 405, "application/json; charset=utf-8", "{\"error\":\"Use GET.\"}");
            return;
        }
        String data = MAP_DATA_CACHE.get(path);
        if (data == null) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(MAP_DATA_URLS.get(path)))
                .timeout(Duration.ofSeconds(20))
                .header("User-Agent", "OrbitLocationGlobe/1.0")
                .GET()
                .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                send(exchange, 502, "application/json; charset=utf-8", "{\"error\":\"Map label data returned HTTP " + response.statusCode() + ".\"}");
                return;
            }
            data = response.body();
            MAP_DATA_CACHE.put(path, data);
        }
        send(exchange, 200, "application/geo+json; charset=utf-8", data);
    }

    private static void serveNews(HttpExchange exchange) throws Exception {
        String query = queryParameter(exchange.getRequestURI().getRawQuery(), "q");
        if (query == null || query.isBlank() || query.length() > 200) {
            send(exchange, 400, "application/json; charset=utf-8", "{\"error\":\"A place search query is required.\"}");
            return;
        }
        CachedFeed cached = CACHE.get(query);
        List<Article> articles;
        if (cached != null && Duration.between(cached.cachedAt(), Instant.now()).toMinutes() < 5) {
            articles = cached.articles();
        } else {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            URI feedUri = URI.create("https://news.google.com/rss/search?q=" + encodedQuery + "&hl=en-US&gl=US&ceid=US%3Aen");
            HttpRequest request = HttpRequest.newBuilder(feedUri)
                .timeout(Duration.ofSeconds(15))
                .header("User-Agent", "OrbitLocationNews/1.0")
                .header("Accept", "application/rss+xml, application/xml")
                .GET()
                .build();
            HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                send(exchange, 502, "application/json; charset=utf-8", "{\"error\":\"Google News RSS returned HTTP " + response.statusCode() + ".\"}");
                return;
            }
            articles = parseFeed(response.body());
            CACHE.put(query, new CachedFeed(Instant.now(), articles));
        }
        StringBuilder json = new StringBuilder("{\"provider\":\"Google News\",\"articles\":[");
        for (int i = 0; i < articles.size(); i++) {
            if (i > 0) json.append(',');
            Article article = articles.get(i);
            json.append("{\"title\":").append(jsonString(article.title()))
                .append(",\"url\":").append(jsonString(article.url()))
                .append(",\"source\":").append(jsonString(article.source()))
                .append(",\"publishedAt\":").append(jsonString(article.publishedAt()))
                .append('}');
        }
        json.append("]}");
        send(exchange, 200, "application/json; charset=utf-8", json.toString());
    }

    private static List<Article> parseFeed(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        Element channel = factory.newDocumentBuilder()
            .parse(new InputSource(new StringReader(xml)))
            .getDocumentElement();
        NodeList items = channel.getElementsByTagName("item");
        List<Article> articles = new ArrayList<>();
        Instant cutoff = Instant.now().minus(Duration.ofDays(7));
        for (int i = 0; i < items.getLength() && articles.size() < 20; i++) {
            Element item = (Element) items.item(i);
            String title = tagText(item, "title");
            String url = tagText(item, "link");
            String publishedAt = tagText(item, "pubDate");
            if (title.isBlank() || !url.startsWith("https://news.google.com/") || publishedAt.isBlank()) continue;
            try {
                Instant published = ZonedDateTime.parse(publishedAt, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
                if (published.isAfter(Instant.now()) || published.isBefore(cutoff)) continue;
            } catch (DateTimeParseException ignored) {
                continue;
            }
            articles.add(new Article(title, url, tagText(item, "source"), publishedAt));
        }
        return List.copyOf(articles);
    }

    private static String tagText(Element parent, String tag) {
        NodeList nodes = parent.getElementsByTagName(tag);
        return nodes.getLength() == 0 ? "" : nodes.item(0).getTextContent().trim();
    }

    private static String queryParameter(String rawQuery, String name) {
        if (rawQuery == null) return null;
        for (String part : rawQuery.split("&")) {
            String[] pair = part.split("=", 2);
            if (pair.length == 2 && pair[0].equals(name)) {
                return java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private static String jsonString(String value) {
        StringBuilder escaped = new StringBuilder("\"");
        for (char character : value.toCharArray()) {
            switch (character) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (character < 0x20) escaped.append(String.format("\\u%04x", (int) character));
                    else escaped.append(character);
                }
            }
        }
        return escaped.append('"').toString();
    }

    private static void send(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] content = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, content.length);
        exchange.getResponseBody().write(content);
    }
}
