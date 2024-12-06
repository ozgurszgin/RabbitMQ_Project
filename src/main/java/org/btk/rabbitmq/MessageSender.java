package org.btk.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.btk.conf.Connector;
import org.btk.pojo.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeoutException;

public class MessageSender implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MessageSender.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5672;
    private static final String DEFAULT_USERNAME = "guest";
    private static final String DEFAULT_PASSWORD = "guest";
    private static final String queueName = "testQueue";
    private static final int MAX_RETRIES = 10; // Maksimum yeniden bağlanma denemesi
    private static final int RETRY_INTERVAL = 5000; // Yeniden bağlanmadan önce bekleme süresi (5 saniye)
    private Queue<Message> messagesQueue;
    private ObjectMapper objectMapper;

    private final Connector connector;
    private boolean isKeepAlive;

    public MessageSender() {
        this.connector = new Connector(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USERNAME, DEFAULT_PASSWORD);
        this.messagesQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
       // while (true) {
            sendMessages();
       // }
    }

    public void addMessage(String state, String response, java.util.List<String> files) {
        Message message = new Message();
        message.setState(state);
        message.setResponse(response);
        message.setFiles(files);
        messagesQueue.offer(message);
        logger.info("Mesaj MessagesQueue'ya eklendi : {}", message);
    }

    public void sendMessages() {

        if (!messagesQueue.isEmpty()) {

            Connection connection = null;
            Channel channel = null;


            int retries = 0;
           // while (retries < MAX_RETRIES) {
                try {
                    connection = connector.createConnection();
                    channel = connection.createChannel();
                    channel.queueDeclare(queueName, true, false, false, null);

                    Message message = messagesQueue.peek();
                    String jsonMessage = message.toJson();
                    channel.basicPublish("", queueName, null, jsonMessage.getBytes());
                    logger.info("Mesaj gönderildi: {}", message);

                    // Mesaj başarılı şekilde gönderildiyse, kuyruktan çıkar
                    messagesQueue.poll();

               //     break;
                } catch (IOException | TimeoutException e) {
                    retries++;
                    logger.error("Mesaj gönderme hatası: {}. Yeniden bağlanma denemesi {}/{}", e.getMessage(), retries, MAX_RETRIES);

//                    if (retries < MAX_RETRIES) {
//                        try {
//                            Thread.sleep(RETRY_INTERVAL);
//                        } catch (InterruptedException ie) {
//                            Thread.currentThread().interrupt();
//                        }
//                    }
//                } finally {
//
//                    // Kanal ve bağlantıyı yalnızca kuyruk boşsa kapat
//                    if (messagesQueue.isEmpty()) {
//                        try {
//                            if (channel != null && channel.isOpen()) {
//                                channel.close();
//                                logger.info("RabbitMQ kanal kapatıldı.");
//                            }
//                        } catch (IOException | TimeoutException e) {
//                            logger.error("Kanal kapama hatası: {}", e.getMessage());
//                        }
//
//                        try {
//                            if (connection != null && connection.isOpen()) {
//                                connection.close();
//                                logger.info("RabbitMQ bağlantısı kapatıldı.");
//                            }
//                        } catch (IOException e) {
//                            logger.error("Bağlantı kapama hatası: {}", e.getMessage());
//                        }
//                    }
                }
            }
//
            else {
            logger.info("MessageQueue boş, bağlantı kurulmadı.");
        }
    }

    public void closeConnection() {
        connector.closeConnection();
    }
}
