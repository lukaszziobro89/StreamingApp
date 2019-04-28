package notifications;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

public class Telegram {
    public static void sendNotification(String message) {

        String urlString = "https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s";

        String apiToken = "808949707:AAGLOvkvf94ydSY7pKF9w3F4wVMimeYdorU";

        String chatId = "@grafana_luq89";

        urlString = String.format(urlString, apiToken, chatId, message);

        try {
            URL url = new URL(urlString);
            URLConnection conn = url.openConnection();

            StringBuilder sb = new StringBuilder();
            InputStream is = new BufferedInputStream(conn.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String inputLine = "";
            while ((inputLine = br.readLine()) != null) {
                sb.append(inputLine);
            }
            String response = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}