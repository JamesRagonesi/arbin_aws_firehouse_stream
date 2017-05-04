package com.forgenano.datastream.arbin;

import com.forgenano.datastream.model.ArbinChannelEvent;

/**
 * Created by michael on 4/10/17.
 */
public interface ArbinDataConsumer {

    public void consume(ArbinChannelEvent arbinChannelEvent);
}
