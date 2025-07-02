import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.concurrent.*;

public class SignalMessageReceiver {

    private static final String SIGNAL_API_URL_TEMPLATE = "http://localhost:7070/v1/receive/%s";
    private static final String DB_URL = "jdbc:sqlite:messages.db";

    private final ObjectMapper objectMapper = new ObjectMapper();

	private final String phoneNumber;
	private final String signalApiUrl;

	public SignalMessageReceiver(String phoneNumber) {
		this.phoneNumber = phoneNumber;
		this.signalApiUrl = String.format(SIGNAL_API_URL_TEMPLATE, phoneNumber);
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Usage: java SignalMessageReceiver <phoneNumber>");
			System.exit(1);
		}

		String phoneNumber = args[0];  // e.g., "+1234567890"
		SignalMessageReceiver receiver = new SignalMessageReceiver(phoneNumber);
		receiver.initDatabase();
		receiver.startPolling();
	}

    private void initDatabase() {
        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            String sql = "CREATE TABLE IF NOT EXISTS messages (" +
                         "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                         "source TEXT," +
                         "timestamp INTEGER," +
                         "message TEXT)";
            stmt.execute(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void startPolling() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::fetchAndStoreMessages, 0, 10, TimeUnit.SECONDS);
    }

    private void fetchAndStoreMessages() {
    try {
        HttpURLConnection conn = (HttpURLConnection) new URL(signalApiUrl).openConnection();
        conn.setRequestMethod("GET");

        if (conn.getResponseCode() != 200) {
            System.err.println("Failed to get messages: HTTP " + conn.getResponseCode());
            return;
        }

        InputStream responseStream = conn.getInputStream();
        JsonNode root = objectMapper.readTree(responseStream);

        if (root.isArray()) {
            for (JsonNode messageNode : root) {
                JsonNode envelope = messageNode.path("envelope");
                if (envelope.isMissingNode()) continue;

                String source = envelope.path("source").asText(); // or "sourceNumber"
                long timestamp = envelope.path("timestamp").asLong();

                JsonNode dataMessage = envelope.path("dataMessage");
                if (dataMessage.isMissingNode()) continue;

                String body = dataMessage.path("message").asText();

                if (!body.isBlank()) {
                    storeMessage(source, timestamp, body);
                }
            }
        }

    } catch (Exception e) {
        System.err.println("Error fetching messages: " + e.getMessage());
    }
}

    private void storeMessage(String source, long timestamp, String message) {
        String sql = "INSERT INTO messages(source, timestamp, message) VALUES (?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, source);
            pstmt.setLong(2, timestamp);
            pstmt.setString(3, message);
            pstmt.executeUpdate();

            System.out.println("Stored message from " + source);

        } catch (SQLException e) {
            System.err.println("Database insert error: " + e.getMessage());
        }
    }
}
