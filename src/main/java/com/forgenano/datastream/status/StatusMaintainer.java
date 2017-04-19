package com.forgenano.datastream.status;

/**
 * Created by michael on 4/18/17.
 */
public class StatusMaintainer {

    private static StatusMaintainer instance;

    public static StatusMaintainer getSingleton() {
        if (instance == null)
            return new StatusMaintainer();

        return instance;
    }
}
