package net.javaguides.orderservice.kafka;

import net.javaguides.basedomains.dto.OrderEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

//import java.util.logging.Logger;

@Service
public class OrderProducer {      //created spring bean class.
    private static final Logger LOGGER= (Logger) LoggerFactory.getLogger(OrderProducer.class);
    private NewTopic topic;     //injected newtopic and kafkaTemplet classes
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Autowired
    public OrderProducer(NewTopic topic, KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }
    public void sendMessage(OrderEvent event){                 //created sendMessage method.
        LOGGER.info(String.format("Order event => %s", event.toString()));

        //create Message
        Message<OrderEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC,topic.name())
                .build();
        kafkaTemplate.send(message);               //used kafkatemp to send message to kafkaTopic.
    }
}
