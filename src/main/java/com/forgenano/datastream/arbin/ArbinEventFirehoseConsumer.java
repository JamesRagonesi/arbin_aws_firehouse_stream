package com.forgenano.datastream.arbin;

import com.forgenano.datastream.aws.ArbinDataFirehoseClient;
import com.forgenano.datastream.model.ArbinChannelEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by michael on 4/10/17.
 */
public class ArbinEventFirehoseConsumer implements ArbinDataConsumer {

    private static final Logger log = LoggerFactory.getLogger(ArbinEventFirehoseConsumer.class);

    private static final boolean SYNCHRONOUSLY_WAIT_FOR_RESULT = false; // If true, this blocks synchronously

    private ArbinDataFirehoseClient arbinDataFirehoseClient;

    public ArbinEventFirehoseConsumer(ArbinDataFirehoseClient arbinDataFirehoseClient) {
        this.arbinDataFirehoseClient = arbinDataFirehoseClient;
    }

    @Override
    public void consume(ArbinChannelEvent arbinChannelEvent) {
        log.info("Consumed arbin data event: " + arbinChannelEvent.toString());

        this.arbinDataFirehoseClient.writeArbinEvent(arbinChannelEvent, SYNCHRONOUSLY_WAIT_FOR_RESULT);
    }
}
