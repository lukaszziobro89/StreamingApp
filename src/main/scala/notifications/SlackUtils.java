package notifications;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import java.io.IOException;

public class SlackUtils {
    private static String slackWebhookUrl = "https://hooks.slack.com/services/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";

    private static void sendMessage(SlackMessage message) {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(slackWebhookUrl);

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(message);
            StringEntity entity = new StringEntity(json);
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            client.execute(httpPost);
            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void sendMessageToChannel(String channelName, String message){
        SlackMessage slackMessage =  new SlackMessage(
                channelName,
                message,
                ":slightly_smiling_face:");
        SlackUtils.sendMessage(slackMessage);
    }
}
