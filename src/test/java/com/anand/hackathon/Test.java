import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PaymentValidator {

    public static void main(String[] args) {
        // Define start and end dates
        String startDate = "2024-09-01T00:00:00"; // Start date (YYYY-MM-DDTHH:MM:SS)
        String endDate = "2024-09-05T23:59:59";   // End date (YYYY-MM-DDTHH:MM:SS)

        // Define the cron expression (example: "0 0 * * *" for every midnight)
        String cronExpression = "0 0 * * *"; // Adjust according to your cron job

        // Validate payments executed based on the cron job
        validatePaymentExecutions(startDate, endDate, cronExpression);
    }

    public static void validatePaymentExecutions(String startDate, String endDate, String cronExpression) {
        // Define date format
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        // Parse the input dates
        LocalDateTime start = LocalDateTime.parse(startDate, dateTimeFormatter);
        LocalDateTime end = LocalDateTime.parse(endDate, dateTimeFormatter);

        // Generate expected trigger times based on the cron expression
        List<ZonedDateTime> expectedTriggers = getCronTriggers(cronExpression, start, end);

        // Retrieve executed payments within the specified date range using an API call
        List<LocalDateTime> executedPayments = getExecutedPaymentsFromApi(startDate, endDate);

        // Validate the number of triggers
        System.out.println("Expected triggers: " + expectedTriggers.size());
        System.out.println("Actual executed payments: " + executedPayments.size());

        if (executedPayments.size() == expectedTriggers.size()) {
            System.out.println("Validation successful: Executed payments match expected triggers.");
        } else {
            System.out.println("Validation failed: Executed payments do not match expected triggers.");
        }
    }

    public static List<ZonedDateTime> getCronTriggers(String cronExpression, LocalDateTime start, LocalDateTime end) {
        List<ZonedDateTime> triggers = new ArrayList<>();
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        Cron cron = parser.parse(cronExpression);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);

        ZonedDateTime nextExecution = ZonedDateTime.of(start, ZoneId.systemDefault());
        ZonedDateTime endZoned = ZonedDateTime.of(end, ZoneId.systemDefault());

        // Find all trigger times within the given range
        while (nextExecution.isBefore(endZoned) || nextExecution.isEqual(endZoned)) {
            Optional<ZonedDateTime> next = executionTime.nextExecution(nextExecution);
            if (next.isPresent() && !next.get().isAfter(endZoned)) {
                triggers.add(next.get());
                nextExecution = next.get();
            } else {
                break;
            }
        }
        return triggers;
    }

    public static List<LocalDateTime> getExecutedPaymentsFromApi(String startDate, String endDate) {
        List<LocalDateTime> paymentExecutions = new ArrayList<>();
        String apiUrl = "https://api.yourservice.com/payments?startDate=" + startDate + "&endDate=" + endDate; // Adjust API URL

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .GET()
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                // Parse JSON response
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(response.body());

                // Assuming the API returns a JSON array of payments with "execution_date"
                for (JsonNode node : rootNode) {
                    String executionDateStr = node.get("execution_date").asText();
                    LocalDateTime executionDate = LocalDateTime.parse(executionDateStr, DateTimeFormatter.ISO_DATE_TIME);
                    paymentExecutions.add(executionDate);
                }
            } else {
                System.err.println("Error: Received non-200 response code: " + response.statusCode());
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("Error fetching payments from API: " + e.getMessage());
        }

        return paymentExecutions;
    }
}
