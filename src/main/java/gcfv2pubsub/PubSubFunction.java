package gcfv2pubsub;

import com.google.cloud.functions.CloudEventsFunction;
//import com.google.cloud.functions.HttpResponse;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.gson.Gson;
import io.cloudevents.CloudEvent;

import java.sql.*;
import java.util.Base64;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class PubSubFunction implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());

  @Override
  public void accept(CloudEvent event) {
    String cloudEventData = new String(event.getData().toBytes());
    Gson gson = new Gson();
    MessagePublishedData data = gson.fromJson(cloudEventData, MessagePublishedData.class);
    Message message = data.getMessage();
    String encodedData = message.getData();
    String decodedData = new String(Base64.getDecoder().decode(encodedData));

    // Assume the message contains user email.
    UserData userData = gson.fromJson(decodedData, UserData.class);
    String userEmail = userData.getUsername();

    String token = UUID.randomUUID().toString();

    String verificationLink = LinkGenerator.generate(userEmail, token);
    EmailService.sendEmail(userEmail, verificationLink);
    DatabaseLogger.logEmail(userEmail, token);
  }

  static class LinkGenerator {
    public static String generate(String email, String token) {
      //String token = UUID.randomUUID().toString();
      return "http://cloudappbysandesh.me:8080/verify?token=" + token ;
    }
  }

  static class EmailService {
    private static final Logger logger = Logger.getLogger(EmailService.class.getName());
    private static final String MAILGUN_DOMAIN = System.getenv("MAILGUN_DOMAIN");
    private static final String MAILGUN_API_KEY = System.getenv("MAILGUN_API_KEY");
    public static void sendEmail(String to, String verificationLink) {
      try {
        HttpClient httpClient = HttpClients.createDefault();
        HttpPost postRequest = new HttpPost("https://api.mailgun.net/v3/" + MAILGUN_DOMAIN + "/messages");

        // Set authentication and headers
        String encoding = Base64.getEncoder().encodeToString(("api:" + MAILGUN_API_KEY).getBytes());
        postRequest.setHeader("Authorization", "Basic " + encoding);

        // Prepare data
        StringEntity input = new StringEntity(
                "from=Your App <mailgun@" + MAILGUN_DOMAIN + ">&to=" + to +
                        "&subject=Email Verification&text=Please verify your email using this link: " + verificationLink
        );
        input.setContentType("application/x-www-form-urlencoded");
        postRequest.setEntity(input);

        // Send request
        HttpResponse response = httpClient.execute(postRequest);
        if (response.getStatusLine().getStatusCode() != 200) {
          logger.warning("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
          logger.warning("Response: " + EntityUtils.toString(response.getEntity()));
        } else {
          logger.info("Email sent successfully to " + to);
        }
      } catch (Exception e) {
        logger.severe("Error sending email: " + e.getMessage());
      }
    }
  }

  static class DatabaseLogger {
    public static void logEmail(String email, String link) {
      // Replace with your actual database connection details
      String jdbcUrl = "jdbc:mysql://"+ System.getenv("DB_HOST") + ":3306/"+ System.getenv("DB_NAME") +"?createDatabaseIfNotExist=true";
      String username = System.getenv("DB_USER");
      String password = System.getenv("DB_PASSWORD");

      String sql = "INSERT INTO token (email, link, verified, exptime) VALUES (?, ?, ?, ?)";


      try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
           PreparedStatement pstmt = conn.prepareStatement(sql)) {

        Timestamp expirationTime = new Timestamp(System.currentTimeMillis() + 2 * 60 * 1000); // 2 minutes

        pstmt.setString(1, email);
        pstmt.setString(2, link);
        pstmt.setBoolean(3, false);
        pstmt.setTimestamp(4, expirationTime);
        pstmt.executeUpdate();
        logger.info("Logged email details to database.");

      } catch (Exception e) {
        logger.severe("Database logging error: " + e.getMessage());
      }
    }
  }
}