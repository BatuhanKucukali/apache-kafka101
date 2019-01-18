package com.arch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerService {

    @KafkaListener(topics = "log", groupId = "group_id")
    public void consume(String message) {
        log.info(String.format("Consumed message -> %s", message));
    }

    @KafkaListener(topics = "log", groupId = "group_id")
    public void consumeWithPartition(@Payload String message,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info(String.format("Consumed message -> %s, Partition Id -> %d", message, partition));
    }

}
