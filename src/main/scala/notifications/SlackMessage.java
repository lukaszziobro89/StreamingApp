package notifications;

import java.io.Serializable;

public class SlackMessage implements Serializable {
    private String channel;
    private String text;
    private String icon_emoji;

    public SlackMessage(String channel, String text, String icon_emoji) {
        this.channel = channel;
        this.text = text;
        this.icon_emoji = icon_emoji;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getIcon_emoji() {
        return icon_emoji;
    }

    public void setIcon_emoji(String icon_emoji) {
        this.icon_emoji = icon_emoji;
    }
}
