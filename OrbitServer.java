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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
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
    private static final Map<String, CachedWildfireFeed> WILDFIRE_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, CachedEarthquakeFeed> EARTHQUAKE_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, CachedConditionsFeed> CONDITIONS_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, CachedStormFeed> HURRICANE_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, CachedStormFeed> TORNADO_CACHE = new ConcurrentHashMap<>();
    private static final String FIRMS_SOURCE_URL = "https://firms.modaps.eosdis.nasa.gov/map/";
    private static final Map<String, String> MAP_DATA_URLS = Map.of(
        "/api/map/countries", "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson",
        "/api/map/cities", "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_populated_places_simple.geojson"
    );

    private record Article(String title, String url, String source, String publishedAt) {}
    private record CachedFeed(Instant cachedAt, List<Article> articles) {}
    private record CachedFlightFeed(Instant cachedAt, String json) {}
    private record CachedWildfireFeed(Instant cachedAt, String json) {}
    private record CachedEarthquakeFeed(Instant cachedAt, String json) {}
    private record CachedConditionsFeed(Instant cachedAt, String json) {}
    private record CachedStormFeed(Instant cachedAt, String json) {}
    private record FlightProvider(String name, String baseUrl) {}
    private record TornadoReport(String id, String time, String scale, String location, String county,
        String state, double latitude, double longitude, String comments) {}
    private record FireCell(int latitude, int longitude) {}

    private static final class FireCluster {
        private double latitudeTotal;
        private double longitudeTotal;
        private double frpTotal;
        private double maxFrp;
        private int detections;
        private int highConfidenceDetections;
        private Instant latestDetection;

        private void add(double latitude, double longitude, double frp, boolean highConfidence, Instant detectedAt) {
            latitudeTotal += latitude;
            longitudeTotal += longitude;
            frpTotal += frp;
            maxFrp = Math.max(maxFrp, frp);
            detections++;
            if (highConfidence) highConfidenceDetections++;
            if (latestDetection == null || detectedAt.isAfter(latestDetection)) latestDetection = detectedAt;
        }

        private double latitude() {
            return latitudeTotal / detections;
        }

        private double longitude() {
            return longitudeTotal / detections;
        }
    }

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
            if (path.equals("/api/earthquakes")) {
                serveEarthquakes(exchange);
                return;
            }
            if (path.equals("/api/wildfires")) {
                serveWildfires(exchange);
                return;
            }
            if (path.equals("/api/conditions")) {
                serveConditions(exchange);
                return;
            }
            if (path.equals("/api/hurricanes")) {
                serveHurricanes(exchange);
                return;
            }
            if (path.equals("/api/tornadoes")) {
                serveTornadoes(exchange);
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

    private static void serveConditions(HttpExchange exchange) throws IOException {
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
            send(exchange, 400, "application/json; charset=utf-8",
                "{\"error\":\"Valid latitude and longitude are required.\"}");
            return;
        }
        if (!Double.isFinite(latitude) || latitude < -90 || latitude > 90 ||
            !Double.isFinite(longitude) || longitude < -180 || longitude > 180) {
            send(exchange, 400, "application/json; charset=utf-8",
                "{\"error\":\"Coordinates must be within ±90° latitude and ±180° longitude.\"}");
            return;
        }

        String cacheKey = String.format(java.util.Locale.ROOT, "%.3f,%.3f", latitude, longitude);
        Instant now = Instant.now();
        CachedConditionsFeed cached = CONDITIONS_CACHE.get(cacheKey);
        if (cached != null && Duration.between(cached.cachedAt(), now).toMinutes() < 20) {
            send(exchange, 200, "application/json; charset=utf-8", cached.json());
            return;
        }

        String point = String.format(java.util.Locale.ROOT, "latitude=%.5f&longitude=%.5f", latitude, longitude);
        HttpRequest weatherRequest = HttpRequest.newBuilder(URI.create(
                "https://api.open-meteo.com/v1/forecast?" + point +
                    "&current=temperature_2m,relative_humidity_2m,precipitation&temperature_unit=celsius&timezone=UTC"))
            .timeout(Duration.ofSeconds(20))
            .header("User-Agent", "BetterEarthConditions/1.0")
            .GET()
            .build();
        HttpRequest airRequest = HttpRequest.newBuilder(URI.create(
                "https://air-quality-api.open-meteo.com/v1/air-quality?" + point +
                    "&current=us_aqi&timezone=UTC"))
            .timeout(Duration.ofSeconds(20))
            .header("User-Agent", "BetterEarthConditions/1.0")
            .GET()
            .build();

        String weather;
        String weatherSource = "Open-Meteo";
        try {
            HttpResponse<String> weatherResponse = sendWithRetry(weatherRequest, 3);
            if (weatherResponse.statusCode() == 200 && isOpenMeteoWeather(weatherResponse.body())) {
                weather = weatherResponse.body();
            } else {
                weather = null;
            }
        } catch (IOException error) {
            weather = null;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The weather request was interrupted.\"}");
            return;
        }

        if (weather == null) {
            HttpRequest fallbackWeatherRequest = HttpRequest.newBuilder(URI.create(
                    String.format(java.util.Locale.ROOT,
                        "https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=%.5f&lon=%.5f",
                        latitude, longitude)))
                .timeout(Duration.ofSeconds(20))
                .header("User-Agent", "BetterEarth/1.0 (global weather fallback)")
                .GET()
                .build();
            try {
                HttpResponse<String> fallbackResponse = sendWithRetry(fallbackWeatherRequest, 2);
                if (fallbackResponse.statusCode() == 200) {
                    weather = normalizeMetWeather(fallbackResponse.body(), latitude, longitude);
                    if (weather != null) weatherSource = "MET Norway";
                }
            } catch (IOException error) {
                weather = null;
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                send(exchange, 502, "application/json; charset=utf-8",
                    "{\"error\":\"The global weather fallback request was interrupted.\"}");
                return;
            }
        }

        if (weather == null) {
            if (cached != null) {
                String staleData = cached.json().replaceFirst(
                    "\"stale\":false", "\"stale\":true");
                send(exchange, 200, "application/json; charset=utf-8", staleData);
                return;
            }
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"Current weather is temporarily unavailable from both global weather providers.\"}");
            return;
        }

        String airQuality = "{\"current\":{\"us_aqi\":null,\"time\":null}}";
        String airQualitySource = "unavailable";
        String airQualityError = "";
        try {
            HttpResponse<String> airResponse = sendWithRetry(airRequest, 3);
            if (airResponse.statusCode() == 200 && isConditionsJson(airResponse.body(), "\"current\"")) {
                airQuality = airResponse.body();
                airQualitySource = "Open-Meteo";
            } else {
                airQualityError = "Open-Meteo air quality returned HTTP " + airResponse.statusCode() + ".";
            }
        } catch (IOException error) {
            airQualityError = "The global air-quality feed could not be reached.";
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The air-quality request was interrupted.\"}");
            return;
        }

        String data = "{\"provider\":\"Global weather feeds\",\"weatherSource\":" + jsonString(weatherSource) +
            ",\"airQualitySource\":" + jsonString(airQualitySource) +
            ",\"airQualityError\":" + jsonString(airQualityError) +
            ",\"stale\":false,\"weather\":" + weather + ",\"airQuality\":" + airQuality + "}";
        if (CONDITIONS_CACHE.size() >= 500) {
            CONDITIONS_CACHE.entrySet().stream()
                .min(Map.Entry.comparingByValue(java.util.Comparator.comparing(CachedConditionsFeed::cachedAt)))
                .ifPresent(oldest -> CONDITIONS_CACHE.remove(oldest.getKey(), oldest.getValue()));
        }
        CONDITIONS_CACHE.put(cacheKey, new CachedConditionsFeed(Instant.now(), data));
        send(exchange, 200, "application/json; charset=utf-8", data);
    }

    private static HttpResponse<String> sendWithRetry(HttpRequest request, int maxAttempts)
        throws IOException, InterruptedException {
        HttpResponse<String> response = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if ((response.statusCode() != 429 && response.statusCode() < 500) || attempt == maxAttempts) {
                return response;
            }
            Thread.sleep(250L * attempt);
        }
        return response;
    }

    private static boolean isConditionsJson(String json, String requiredField) {
        return json.startsWith("{") && json.contains(requiredField);
    }

    private static boolean isOpenMeteoWeather(String json) {
        return isConditionsJson(json, "\"current\"") &&
            jsonNumber(json, "temperature_2m") != null &&
            jsonNumber(json, "relative_humidity_2m") != null &&
            jsonNumber(json, "precipitation") != null &&
            jsonStringField(json, "time") != null;
    }

    private static String normalizeMetWeather(String json, double latitude, double longitude) {
        Double temperature = jsonNumber(json, "air_temperature");
        Double humidity = jsonNumber(json, "relative_humidity");
        Double precipitation = jsonNumber(json, "precipitation_amount");
        String time = jsonStringField(json, "time");
        if (temperature == null || humidity == null || time == null) return null;
        String precipJson = precipitation == null ? "null" : precipitation.toString();
        return "{\"latitude\":" + latitude + ",\"longitude\":" + longitude +
            ",\"current_units\":{\"time\":\"iso8601\",\"temperature_2m\":\"°C\"," +
            "\"relative_humidity_2m\":\"%\",\"precipitation\":\"mm\"}," +
            "\"current\":{\"time\":" + jsonString(time) +
            ",\"temperature_2m\":" + temperature +
            ",\"relative_humidity_2m\":" + humidity +
            ",\"precipitation\":" + precipJson + "}}";
    }

    private static Double jsonNumber(String json, String field) {
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(
            "\"" + java.util.regex.Pattern.quote(field) + "\"\\s*:\\s*(-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)")
            .matcher(json);
        return matcher.find() ? Double.valueOf(matcher.group(1)) : null;
    }

    private static String jsonStringField(String json, String field) {
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(
            "\"" + java.util.regex.Pattern.quote(field) + "\"\\s*:\\s*\"([^\"]+)\"")
            .matcher(json);
        return matcher.find() ? matcher.group(1) : null;
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
            Duration.between(entry.getValue().cachedAt(), now).toSeconds() >= 60);
        String cacheKey = String.format(java.util.Locale.ROOT, "%.3f,%.3f", latitude, longitude);
        CachedFlightFeed cached = FLIGHT_CACHE.get(cacheKey);
        if (cached != null) {
            send(exchange, 200, "application/json; charset=utf-8", cached.json());
            return;
        }

        List<FlightProvider> providers = List.of(
            new FlightProvider("ADSB.lol", "https://api.adsb.lol/v2/lat/%.5f/lon/%.5f/dist/44"),
            new FlightProvider("ADSB.fi", "https://opendata.adsb.fi/api/v2/lat/%.5f/lon/%.5f/dist/44")
        );
        String data = null;
        String bestProvider = null;
        int lastStatus = 502;
        for (FlightProvider provider : providers) {
            String path = String.format(java.util.Locale.ROOT, provider.baseUrl(), latitude, longitude);
            HttpRequest request = HttpRequest.newBuilder(URI.create(path))
                .timeout(Duration.ofSeconds(15))
                .header("User-Agent", "BetterEarth/1.0 (global live aircraft map)")
                .header("Accept", "application/json")
                .GET()
                .build();
            HttpResponse<String> response;
            try {
                response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            } catch (IOException error) {
                System.err.println(provider.name() + " aircraft feed request failed: " + error.getMessage());
                continue;
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                send(exchange, 502, "application/json; charset=utf-8",
                    "{\"error\":\"The live aircraft request was interrupted.\"}");
                return;
            }
            if (response.statusCode() != 200) {
                lastStatus = response.statusCode();
                System.err.println(provider.name() + " aircraft feed returned HTTP " + lastStatus + ".");
                continue;
            }
            String providerData = response.body();
            if (provider.name().equals("ADSB.fi")) {
                providerData = providerData.replaceFirst("\"aircraft\"\\s*:", "\"ac\":");
                java.util.regex.Matcher nowMatcher =
                    java.util.regex.Pattern.compile("\"now\"\\s*:\\s*(\\d+)").matcher(providerData);
                if (nowMatcher.find()) {
                    try {
                        long timestamp = Long.parseLong(nowMatcher.group(1));
                        if (timestamp < 100_000_000_000L) {
                            providerData = providerData.substring(0, nowMatcher.start(1)) +
                                (timestamp * 1000) + providerData.substring(nowMatcher.end(1));
                        }
                    } catch (NumberFormatException ignored) {
                        System.err.println("ADSB.fi returned an invalid timestamp.");
                        continue;
                    }
                }
            }
            if (!providerData.startsWith("{") || !providerData.contains("\"ac\"")) {
                System.err.println(provider.name() + " aircraft feed returned invalid data.");
                continue;
            }
            if (data == null) {
                data = providerData;
                bestProvider = provider.name();
            }
            if (!providerData.matches("(?s).*\"ac\"\\s*:\\s*\\[\\s*\\].*")) {
                data = providerData;
                bestProvider = provider.name();
                break;
            }
        }
        if (data == null) {
            int status = lastStatus == 429 ? 429 : 502;
            String message = status == 429
                ? "All live aircraft feeds are rate-limited. Please wait briefly and try again."
                : "All live aircraft feeds are temporarily unavailable.";
            send(exchange, status, "application/json; charset=utf-8",
                "{\"error\":" + jsonString(message) + "}");
            return;
        }
        data = "{\"provider\":" + jsonString(bestProvider) + "," + data.substring(1);
        if (FLIGHT_CACHE.size() >= 500) {
            FLIGHT_CACHE.entrySet().stream()
                .min(Map.Entry.comparingByValue(java.util.Comparator.comparing(CachedFlightFeed::cachedAt)))
                .ifPresent(oldest -> FLIGHT_CACHE.remove(oldest.getKey(), oldest.getValue()));
        }
        FLIGHT_CACHE.put(cacheKey, new CachedFlightFeed(Instant.now(), data));
        send(exchange, 200, "application/json; charset=utf-8", data);
    }

    private static void serveEarthquakes(HttpExchange exchange) throws IOException {
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
            send(exchange, 400, "application/json; charset=utf-8",
                "{\"error\":\"Valid latitude and longitude are required.\"}");
            return;
        }
        if (!Double.isFinite(latitude) || latitude < -90 || latitude > 90 ||
            !Double.isFinite(longitude) || longitude < -180 || longitude > 180) {
            send(exchange, 400, "application/json; charset=utf-8",
                "{\"error\":\"Coordinates must be within ±90° latitude and ±180° longitude.\"}");
            return;
        }
        boolean useEmsc = "international".equalsIgnoreCase(queryParameter(rawQuery, "region"));
        String provider = useEmsc ? "EMSC" : "USGS";
        String cacheKey = provider + ":" + String.format(java.util.Locale.ROOT, "%.4f,%.4f", latitude, longitude);
        Instant now = Instant.now();
        CachedEarthquakeFeed cached = EARTHQUAKE_CACHE.get(cacheKey);
        if (cached != null && Duration.between(cached.cachedAt(), now).toMinutes() < 5) {
            send(exchange, 200, "application/geo+json; charset=utf-8", cached.json());
            return;
        }

        String startTime = URLEncoder.encode(now.minus(Duration.ofHours(48)).toString(), StandardCharsets.UTF_8);
        String endTime = URLEncoder.encode(now.toString(), StandardCharsets.UTF_8);
        String url = useEmsc
            ? String.format(java.util.Locale.ROOT,
                "https://www.seismicportal.eu/fdsnws/event/1/query?format=json&lat=%.6f&lon=%.6f&maxradius=0.724&start=%s&end=%s&orderby=magnitude&limit=500",
                latitude, longitude, startTime, endTime)
            : String.format(java.util.Locale.ROOT,
                "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&latitude=%.6f&longitude=%.6f&maxradiuskm=80.4672&starttime=%s&endtime=%s&orderby=magnitude&limit=500",
                latitude, longitude, startTime, endTime);
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(20))
            .header("User-Agent", "BetterEarthEarthquakeMap/2.0")
            .GET()
            .build();
        HttpResponse<String> response;
        try {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException error) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The " + provider + " earthquake feed could not be reached.\"}");
            return;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The " + provider + " earthquake request was interrupted.\"}");
            return;
        }
        if (response.statusCode() != 200) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"" + provider + " returned HTTP " + response.statusCode() + " for the earthquake search.\"}");
            return;
        }
        String data = response.body();
        if (!data.startsWith("{") || !data.contains("\"features\"")) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"" + provider + " returned invalid earthquake data.\"}");
            return;
        }
        if (EARTHQUAKE_CACHE.size() >= 500) {
            EARTHQUAKE_CACHE.entrySet().stream()
                .min(Map.Entry.comparingByValue(java.util.Comparator.comparing(CachedEarthquakeFeed::cachedAt)))
                .ifPresent(oldest -> EARTHQUAKE_CACHE.remove(oldest.getKey(), oldest.getValue()));
        }
        String responseData = "{\"provider\":" + jsonString(provider) + "," + data.substring(1);
        EARTHQUAKE_CACHE.put(cacheKey, new CachedEarthquakeFeed(Instant.now(), responseData));
        send(exchange, 200, "application/geo+json; charset=utf-8", responseData);
    }

    private static void serveWildfires(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            send(exchange, 405, "application/json; charset=utf-8", "{\"error\":\"Use GET.\"}");
            return;
        }
        String mapKey = System.getenv("FIRMS_MAP_KEY");
        if (mapKey == null || mapKey.isBlank() || !mapKey.matches("[A-Za-z0-9_-]+")) {
            serveWildfireFallback(exchange);
            return;
        }
        Instant now = Instant.now();
        CachedWildfireFeed cached = WILDFIRE_CACHE.get("FIRMS");
        if (cached != null && Duration.between(cached.cachedAt(), now).toMinutes() < 5) {
            send(exchange, 200, "application/json; charset=utf-8", cached.json());
            return;
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(
                "https://firms.modaps.eosdis.nasa.gov/api/area/csv/" + mapKey + "/VIIRS_SNPP_NRT/world/3"))
            .timeout(Duration.ofSeconds(20))
            .header("User-Agent", "BetterEarthWildfireMap/2.0")
            .GET()
            .build();
        HttpResponse<String> response;
        try {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException error) {
            serveWildfireFallback(exchange);
            return;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            serveWildfireFallback(exchange);
            return;
        }
        if (response.statusCode() != 200) {
            serveWildfireFallback(exchange);
            return;
        }
        List<String> lines = response.body().lines().toList();
        if (lines.isEmpty()) {
            serveWildfireFallback(exchange);
            return;
        }
        Map<String, Integer> columns = new HashMap<>();
        List<String> headers = parseCsvRecord(lines.get(0));
        for (int i = 0; i < headers.size(); i++) columns.put(headers.get(i).trim().toLowerCase(), i);
        if (!columns.keySet().containsAll(List.of("latitude", "longitude", "acq_date", "acq_time", "confidence", "frp"))) {
            serveWildfireFallback(exchange);
            return;
        }

        Instant cutoff = now.minus(Duration.ofHours(48));
        Map<FireCell, FireCluster> clusters = new HashMap<>();
        for (int lineNumber = 1; lineNumber < lines.size(); lineNumber++) {
            List<String> row = parseCsvRecord(lines.get(lineNumber));
            if (row.size() < headers.size()) continue;
            try {
                double latitude = Double.parseDouble(row.get(columns.get("latitude")));
                double longitude = Double.parseDouble(row.get(columns.get("longitude")));
                double frp = Double.parseDouble(row.get(columns.get("frp")));
                Instant detectedAt = parseFirmsTimestamp(
                    row.get(columns.get("acq_date")), row.get(columns.get("acq_time")));
                if (!Double.isFinite(latitude) || latitude < -90 || latitude > 90 ||
                    !Double.isFinite(longitude) || longitude < -180 || longitude > 180 ||
                    !Double.isFinite(frp) || frp < 0 || detectedAt == null ||
                    detectedAt.isBefore(cutoff) || detectedAt.isAfter(now)) continue;
                String confidence = row.get(columns.get("confidence")).trim();
                boolean highConfidence = confidence.equalsIgnoreCase("h");
                if (!highConfidence) {
                    try {
                        highConfidence = Double.parseDouble(confidence) >= 80;
                    } catch (NumberFormatException ignored) {
                        // FIRMS confidence may be categorical (l, n, h) or numeric.
                    }
                }
                FireCell cell = new FireCell((int) Math.floor(latitude * 10), (int) Math.floor(longitude * 10));
                clusters.computeIfAbsent(cell, ignored -> new FireCluster())
                    .add(latitude, longitude, frp, highConfidence, detectedAt);
            } catch (NumberFormatException | DateTimeParseException ignored) {
                // Skip malformed source rows; valid rows still form the global feed.
            }
        }

        List<Map.Entry<FireCell, FireCluster>> topClusters = clusters.entrySet().stream()
            .sorted(Map.Entry.<FireCell, FireCluster>comparingByValue(
                Comparator.comparingDouble((FireCluster cluster) -> cluster.frpTotal).reversed()
                    .thenComparing(Comparator.comparingInt((FireCluster cluster) -> cluster.detections).reversed())))
            .limit(5)
            .toList();
        StringBuilder json = new StringBuilder("{\"provider\":\"NASA FIRMS VIIRS SNPP NRT\",\"windowHours\":48,\"events\":[");
        for (int i = 0; i < topClusters.size(); i++) {
            if (i > 0) json.append(',');
            FireCluster cluster = topClusters.get(i).getValue();
            double latitude = cluster.latitude();
            double longitude = cluster.longitude();
            json.append("{\"title\":").append(jsonString("High-activity fire detection cluster #" + (i + 1)))
                .append(",\"latitude\":").append(latitude)
                .append(",\"longitude\":").append(longitude)
                .append(",\"detectionCount\":").append(cluster.detections)
                .append(",\"totalFrp\":").append(cluster.frpTotal)
                .append(",\"maxFrp\":").append(cluster.maxFrp)
                .append(",\"highConfidenceDetections\":").append(cluster.highConfidenceDetections)
                .append(",\"lastDetected\":").append(jsonString(cluster.latestDetection.toString()))
                .append(",\"sourceUrl\":").append(jsonString(FIRMS_SOURCE_URL))
                .append('}');
        }
        json.append("]}");
        String data = json.toString();
        WILDFIRE_CACHE.put("FIRMS", new CachedWildfireFeed(Instant.now(), data));
        send(exchange, 200, "application/json; charset=utf-8", data);
    }

    private static void serveWildfireFallback(HttpExchange exchange) throws IOException {
        Instant now = Instant.now();
        CachedWildfireFeed cached = WILDFIRE_CACHE.get("COPERNICUS_GWIS");
        if (cached != null && Duration.between(cached.cachedAt(), now).toMinutes() < 15) {
            send(exchange, 200, "application/json; charset=utf-8", cached.json());
            return;
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(
                "https://maps.effis.emergency.copernicus.eu/gwis?service=WFS&version=1.1.0&request=GetFeature&typeName=ms%3Anrt.ba.point.today&maxFeatures=10000&outputFormat=application%2Fjson%3B%20subtype%3Dgeojson"))
            .timeout(Duration.ofSeconds(45))
            .header("User-Agent", "BetterEarthWildfireMap/3.0")
            .GET()
            .build();
        HttpResponse<String> response;
        try {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException error) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"Copernicus GWIS wildfire data could not be reached.\"}");
            return;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The Copernicus GWIS wildfire request was interrupted.\"}");
            return;
        }
        String sourceData = response.body();
        if (response.statusCode() != 200 || !sourceData.startsWith("{") || !sourceData.contains("\"features\"")) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"Copernicus GWIS returned invalid wildfire data.\"}");
            return;
        }
        String data = "{\"provider\":\"Copernicus GWIS NRT Burnt Areas\"," + sourceData.substring(1);
        WILDFIRE_CACHE.put("COPERNICUS_GWIS", new CachedWildfireFeed(Instant.now(), data));
        send(exchange, 200, "application/json; charset=utf-8", data);
    }

    private static Instant parseFirmsTimestamp(String date, String time) {
        String digits = time.trim();
        if (digits.isEmpty() || digits.length() > 4) return null;
        int hhmm = Integer.parseInt(digits);
        int hour = hhmm / 100;
        int minute = hhmm % 100;
        if (hour > 23 || minute > 59) return null;
        return LocalDate.parse(date.trim()).atTime(LocalTime.of(hour, minute)).toInstant(ZoneOffset.UTC);
    }

    private static List<String> parseCsvRecord(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char character = line.charAt(i);
            if (character == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    field.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (character == ',' && !quoted) {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(character);
            }
        }
        fields.add(field.toString());
        return fields;
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

    private static void serveHurricanes(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            send(exchange, 405, "application/json; charset=utf-8", "{\"error\":\"Use GET.\"}");
            return;
        }
        Instant now = Instant.now();
        CachedStormFeed cached = HURRICANE_CACHE.get("global");
        if (cached != null && Duration.between(cached.cachedAt(), now).toMinutes() < 15) {
            send(exchange, 200, "application/geo+json; charset=utf-8", cached.json());
            return;
        }

        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        URI feedUri = URI.create("https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH" +
            "?eventtype=TC&fromdate=" + today.minusDays(30) + "&todate=" + today);
        HttpRequest request = HttpRequest.newBuilder(feedUri)
            .timeout(Duration.ofSeconds(25))
            .header("User-Agent", "BetterEarthStormReports/1.0")
            .header("Accept", "application/json")
            .GET()
            .build();
        HttpResponse<String> response;
        try {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException error) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The global GDACS tropical-cyclone feed could not be reached.\"}");
            return;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The GDACS tropical-cyclone request was interrupted.\"}");
            return;
        }
        if (response.statusCode() != 200) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The GDACS tropical-cyclone feed returned HTTP " + response.statusCode() + ".\"}");
            return;
        }
        String data = response.body();
        if (!data.startsWith("{") || !data.contains("\"features\"")) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The GDACS tropical-cyclone feed returned invalid data.\"}");
            return;
        }
        HURRICANE_CACHE.put("global", new CachedStormFeed(now, data));
        send(exchange, 200, "application/geo+json; charset=utf-8", data);
    }

    private static void serveTornadoes(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            send(exchange, 405, "application/json; charset=utf-8", "{\"error\":\"Use GET.\"}");
            return;
        }
        Instant now = Instant.now();
        CachedStormFeed cached = TORNADO_CACHE.get("us");
        if (cached != null && Duration.between(cached.cachedAt(), now).toSeconds() < 300) {
            send(exchange, 200, "application/geo+json; charset=utf-8", cached.json());
            return;
        }

        LocalDate reportDate = LocalDate.now(ZoneOffset.UTC);
        List<String> reportNames = List.of("today_torn.csv", "yesterday_torn.csv");
        List<CompletableFuture<HttpResponse<String>>> requests = new ArrayList<>();
        for (String reportName : reportNames) {
            URI feedUri = URI.create("https://www.spc.noaa.gov/climo/reports/" + reportName);
            HttpRequest request = HttpRequest.newBuilder(feedUri)
                .timeout(Duration.ofSeconds(15))
                .header("User-Agent", "BetterEarthStormReports/1.0 (NOAA SPC public report data)")
                .header("Accept", "text/csv")
                .GET()
                .build();
            requests.add(HTTP.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)));
        }

        List<TornadoReport> reports = new ArrayList<>();
        int successfulFeeds = 0;
        try {
            try {
                CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new)).get(18, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                System.err.println("A NOAA tornado report window failed: " + error.getMessage());
            }
            for (int i = 0; i < requests.size(); i++) {
                CompletableFuture<HttpResponse<String>> future = requests.get(i);
                try {
                    HttpResponse<String> response = future.get();
                    if (response.statusCode() != 200) continue;
                    if (response.body().isBlank()) {
                        successfulFeeds++;
                        continue;
                    }
                    if (!response.body().startsWith("Time,F_Scale,Location,County,State,Lat,Lon,Comments")) {
                        System.err.println("NOAA SPC returned an invalid " + reportNames.get(i) + " report.");
                        continue;
                    }
                    successfulFeeds++;
                    parseTornadoReports(response.body(), reportDate.minusDays(i), now, reports);
                } catch (ExecutionException error) {
                    System.err.println("NOAA SPC " + reportNames.get(i) + " could not be read: " + error.getMessage());
                }
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            requests.forEach(future -> future.cancel(true));
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The NOAA tornado reports request was interrupted.\"}");
            return;
        } catch (TimeoutException error) {
            requests.forEach(future -> future.cancel(true));
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The NOAA tornado reports request timed out.\"}");
            return;
        }
        if (successfulFeeds == 0) {
            send(exchange, 502, "application/json; charset=utf-8",
                "{\"error\":\"The NOAA Storm Prediction Center tornado reports are currently unavailable.\"}");
            return;
        }
        reports.sort(Comparator.comparing(TornadoReport::time).reversed());
        String data = tornadoReportsJson(reports, successfulFeeds < reportNames.size());
        TORNADO_CACHE.put("us", new CachedStormFeed(now, data));
        send(exchange, 200, "application/geo+json; charset=utf-8", data);
    }

    private static void parseTornadoReports(String csv, LocalDate reportPeriodStart, Instant now,
        List<TornadoReport> reports) {
        String[] lines = csv.split("\\R");
        if (lines.length < 2) return;
        Map<String, Integer> columns = new HashMap<>();
        List<String> header = parseCsvLine(lines[0]);
        for (int i = 0; i < header.size(); i++) columns.put(header.get(i).trim(), i);
        for (int i = 1; i < lines.length; i++) {
            List<String> fields = parseCsvLine(lines[i]);
            String rawTime = csvField(fields, columns, "Time");
            String rawLatitude = csvField(fields, columns, "Lat");
            String rawLongitude = csvField(fields, columns, "Lon");
            if (rawTime.isBlank() || rawLatitude.isBlank() || rawLongitude.isBlank()) continue;
            try {
                String normalizedTime = rawTime.replace(":", "").trim();
                LocalTime time = LocalTime.parse(normalizedTime, DateTimeFormatter.ofPattern("HHmm"));
                LocalDate eventDate = time.getHour() < 12 ? reportPeriodStart.plusDays(1) : reportPeriodStart;
                Instant reportedAt = ZonedDateTime.of(eventDate, time, ZoneOffset.UTC).toInstant();
                if (reportedAt.isAfter(now) || reportedAt.isBefore(now.minus(Duration.ofHours(48)))) continue;
                double latitude = Double.parseDouble(rawLatitude);
                double longitude = Double.parseDouble(rawLongitude);
                if (!Double.isFinite(latitude) || latitude < -90 || latitude > 90 ||
                    !Double.isFinite(longitude) || longitude < -180 || longitude > 180) continue;
                String location = csvField(fields, columns, "Location");
                String county = csvField(fields, columns, "County");
                String state = csvField(fields, columns, "State");
                String scale = csvField(fields, columns, "F_Scale");
                String comments = csvField(fields, columns, "Comments");
                String id = eventDate + "-" + normalizedTime + "-" + state + "-" + rawLatitude + "-" + rawLongitude;
                reports.add(new TornadoReport(id, reportedAt.toString(), scale, location, county, state,
                    latitude, longitude, comments));
            } catch (DateTimeParseException | NumberFormatException ignored) {
                continue;
            }
        }
    }

    private static String csvField(List<String> fields, Map<String, Integer> columns, String name) {
        Integer index = columns.get(name);
        return index == null || index >= fields.size() ? "" : fields.get(index).trim();
    }

    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char character = line.charAt(i);
            if (character == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    field.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (character == ',' && !quoted) {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(character);
            }
        }
        fields.add(field.toString());
        return fields;
    }

    private static String tornadoReportsJson(List<TornadoReport> reports, boolean partial) {
        StringBuilder json = new StringBuilder("{\"type\":\"FeatureCollection\"")
            .append(",\"provider\":\"NOAA Storm Prediction Center\"")
            .append(",\"coverage\":\"United States\"")
            .append(",\"partial\":").append(partial)
            .append(",\"sourceUrl\":\"https://www.spc.noaa.gov/climo/reports/\"")
            .append(",\"features\":[");
        for (int i = 0; i < reports.size(); i++) {
            if (i > 0) json.append(',');
            TornadoReport report = reports.get(i);
            json.append("{\"type\":\"Feature\",\"id\":").append(jsonString(report.id()))
                .append(",\"geometry\":{\"type\":\"Point\",\"coordinates\":[")
                .append(report.longitude()).append(',').append(report.latitude())
                .append("]},\"properties\":{\"time\":").append(jsonString(report.time()))
                .append(",\"scale\":").append(jsonString(report.scale()))
                .append(",\"location\":").append(jsonString(report.location()))
                .append(",\"county\":").append(jsonString(report.county()))
                .append(",\"state\":").append(jsonString(report.state()))
                .append(",\"comments\":").append(jsonString(report.comments())).append("}}");
        }
        return json.append("]}").toString();
    }

    private static List<Article> parseFeed(String xml)
        throws ParserConfigurationException, SAXException, IOException {
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
