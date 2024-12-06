package org.btk.conf;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Connector {

    private static final Logger logger = LoggerFactory.getLogger(Connector.class);

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private Connection connection;

    public Connector(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }
    private void setupConnectionFactory(ConnectionFactory factory) {
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setAutomaticRecoveryEnabled(true); // Otomatik yeniden bağlanma
        factory.setNetworkRecoveryInterval(5000); // Yeniden bağlanma denemeleri arası bekleme süresi
    }

    public Connection createConnection() throws IOException, TimeoutException {
        if (connection == null || !connection.isOpen()) {
            ConnectionFactory factory = new ConnectionFactory();
            setupConnectionFactory(factory);
            connection = factory.newConnection();
            logger.info("RabbitMQ bağlantısı oluşturuldu.");
        } else {
            logger.info("Mevcut RabbitMQ bağlantısı kullanılıyor.");
        }
        return connection;
    }
    public Channel createChannel() throws IOException, TimeoutException {
        Connection conn = createConnection();
        return conn.createChannel();
    }

    public void closeConnection() {
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
                logger.info("RabbitMQ bağlantısı kapatıldı.");
            } catch (IOException e) {
                logger.error("Bağlantı kapatma hatası: {}", e.getMessage());
            }
        }
    }

}
