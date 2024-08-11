package com.example.kafkaexample.subscriber;
import com.example.kafkaexample.events.listeners.UpdateEventListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;


@Component
public class EventSubscriber implements ApplicationRunner {

    public final UpdateEventListener updateEventProcessor;

    public final Logger logger = LogManager.getLogger(this.getClass());

    @Autowired
    public EventSubscriber(UpdateEventListener updateEventProcessor) {
        this.updateEventProcessor = updateEventProcessor;
    }

    @Override
    public void run(ApplicationArguments args) {
        logger.info("Starting change stream processor");
        updateEventProcessor.listenToChangeEvent();
    }
}
