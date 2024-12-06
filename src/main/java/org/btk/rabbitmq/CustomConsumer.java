package org.btk.rabbitmq;

import com.rabbitmq.client.*;
import org.btk.pojo.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CustomConsumer extends DefaultConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CustomConsumer.class);

    public CustomConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String jsonMessage = new String(body);
        logger.info("Mesaj alındı: {}", jsonMessage);

        processMessage(jsonMessage);
    }
    private void processMessage(String message) {
        logger.info("Mesaj işleniyor: {}", message);
    }

    @Override
    public void handleConsumeOk(String consumerTag) {
        super.handleConsumeOk(consumerTag);
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        super.handleCancelOk(consumerTag);
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        super.handleShutdownSignal(consumerTag, sig);
    }

    @Override
    public void handleRecoverOk(String consumerTag) {
        super.handleRecoverOk(consumerTag);
    }

}
