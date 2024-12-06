package org.btk;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqTest {
    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setAutomaticRecoveryEnabled(true); // Otomatik bağlantı kurtarma
        factory.setNetworkRecoveryInterval(1000); // Bağlantı kopması sonrası bekleme süresi

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            String queueName = "testQueue";
            channel.queueDeclare(queueName, true, false, false, null);

            sendMessage(channel,queueName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    static void sendMessage(Channel channel,String queueName){


        for (int i=0;i<200;i++) {
            try {
                String message = "Hello, RabbitMQ!"+i;
                channel.basicPublish("", queueName, null, message.getBytes());
                System.out.println(message);
            }catch (Exception e)
            {
                System.out.println(e);
            }


            try {
                Thread.sleep(1000);
            }
            catch (Exception e)
            {
                System.out.println(e);
            }
        }


    }
}
