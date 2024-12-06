package org.btk;

import org.btk.rabbitmq.MessageReceiver;
import org.btk.rabbitmq.MessageSender;
import org.springframework.context.support.MessageSourceAccessor;

import java.util.List;

public class Main {
    public static void main(String[] args) {

        //MessageReceiver messageReceiver = new MessageReceiver();
        MessageSender messageSender = new MessageSender();

        // Alıcı ve gönderici thread'lerini başlat
        Thread senderThread = new Thread(messageSender);
        //Thread receiverThread= new Thread(messageReceiver);


        int messageCount = 0;

        // 100 mesaj gönder ve sonra bitir
        for (int i = 0; i < 100; i++) {
            messageSender.addMessage("Success " + messageCount++, "PartialSuccess", List.of("file" + i));
            try {
                Thread.sleep(00); // 0.5 saniye bekle
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        senderThread.start();
        try {
            Thread.sleep(1000);

        }
        catch (Exception e)
        {
            System.out.println(e);
        }
       // receiverThread.start();

    }
}