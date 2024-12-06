package org.btk.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.btk.conf.Connector;
import org.btk.pojo.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MessageReceiver implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MessageReceiver.class);
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5672;
    private static final String DEFAULT_USERNAME = "guest";
    private static final String DEFAULT_PASSWORD = "guest";
    private static final String queueName = "testQueue";

    private final Connector connector;
    private  Connection connection;
    private  Channel channel;

    public MessageReceiver() {
        this.connector = new Connector(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    @Override
    public void run() {
        try {
            initializeConnectionAndChannel();
            receiveMessages();
        } catch (Exception e) {
            logger.error("Başlangıç hatası: {}", e.getMessage());
        }
    }
    private void initializeConnectionAndChannel() throws Exception {
        // Bağlantı ve kanal sadece bir kez başlatılacak
        if (connection == null || !connection.isOpen()) {
            connection = connector.createConnection();
            logger.info("Bağlantı oluşturuldu: {}", connection);
        }

        if (channel == null || !channel.isOpen()) {
            channel = connection.createChannel();
            logger.info("Kanal oluşturuldu: {}", channel);
        }
    }
    public void receiveMessages() {

        try {

            channel.queueDeclare(queueName, true, false, false, null);//bind işlemi için exchange'e girmemek için böyle yaptım.
            logger.info("Kuyruğa bağlanıldı: {}", queueName);

            CustomConsumer consumer = new CustomConsumer(channel);

            channel.basicConsume(queueName, true, consumer);
            logger.info("{} kuyruğundaki mesajlar dinlenmeye başlandı..", queueName);

        } catch (Exception e) {
            logger.error("Mesaj alma hatası: {}. Bağlantı tekrar deneniyor.", e.getMessage());

            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
                logger.info("Kanal kapatıldı.");
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
                logger.info("Bağlantı kapatıldı.");
            }
        } catch (Exception e) {
            logger.error("Kanal/Bağlantı kapama hatası: {}", e.getMessage());
        }
    }
}
