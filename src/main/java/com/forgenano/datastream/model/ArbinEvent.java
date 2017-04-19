package com.forgenano.datastream.model;

import com.google.gson.Gson;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by michael on 4/7/17.
 */
public class ArbinEvent {

    private static Gson jsonConverter;
    private static DateTimeFormatter dateTimeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.sssZ");

    private static String BuildCompositeId(String testName, long testId, long dataPoint, int channelIndex) {
        StringBuffer id = new StringBuffer();

        id.append(testName);
        id.append("_tid_");
        id.append(testId);
        id.append("_dp_");
        id.append(dataPoint);
        id.append("_ci_");
        id.append(channelIndex);

        return id.toString();
    }

    private static String parseTestNameFromChannelTestName(String channelTestName) {
        return channelTestName.substring(0, channelTestName.length() - 7);
    }

    private String compositeId;
    private long testId;
    private String testName;
    private String channelTestName;
    private String scheduleFileName;
    private long dataPoint;
    private int channelIndex;
    private int daqIndex;
    private int channelType;
    private String creator;
    private double testTime;
    private double stepTime;
    private String dateTime;
    private int stepIndex;
    private int cycleIndex;
    private boolean isFCData;
    private double current;
    private double voltage;
    private double chargeCapacity;
    private double dischargeCapacity;
    private double dV_dt;
    private double internalResistance;
    private double acImpedance;
    private double aciPhaseAngle;

    public ArbinEvent(long testId, String channelTestName, long dataPoint, int channelIndex, int daqIndex,
                      int channelType, String creator, double testTimeRaw, double stepTimeRaw, double dateTimeRaw,
                      int stepIndex, int cycleIndex, boolean isFCData, double current, double voltage,
                      double chargeCapacity, double dischargeCapacity, double dV_dt, double internalResistance,
                      double acImpedance, double aciPhaseAngle, String scheduleFileName) {

        this.testId = testId;
        this.testName = parseTestNameFromChannelTestName(channelTestName);
        this.channelTestName = channelTestName;
        this.compositeId = BuildCompositeId(testName, testId, dataPoint, channelIndex);
        this.scheduleFileName = scheduleFileName;
        this.dataPoint = dataPoint;
        this.channelIndex = channelIndex;
        this.daqIndex = daqIndex;
        this.channelType = channelType;
        this.creator = creator;
        this.testTime = testTimeRaw;
        this.stepTime = stepTimeRaw;
        this.stepIndex = stepIndex;
        this.cycleIndex = cycleIndex;
        this.isFCData = isFCData;
        this.current = current;
        this.voltage = voltage;
        this.chargeCapacity = chargeCapacity;
        this.dischargeCapacity = dischargeCapacity;
        this. dV_dt = dV_dt;
        this.internalResistance = internalResistance;
        this.acImpedance = acImpedance;
        this.aciPhaseAngle = aciPhaseAngle;

        long dateTimeMilliseconds = (long) ((dateTimeRaw - 25569) * 86400 * 1000);

        LocalDateTime dateTime = new LocalDateTime(dateTimeMilliseconds, DateTimeZone.getDefault());

        this.dateTime = dateTime.toString(dateTimeFormatter);
    }

    public String toString() {
        StringBuffer out = new StringBuffer();

        out.append("Arbin Data Point - TestName: ");
        out.append(this.testName);
        out.append(" TestId: ");
        out.append(this.testId);
        out.append(" DataPoint: ");
        out.append(this.dataPoint);
        out.append(" i: ");
        out.append(this.current);
        out.append(" v: ");
        out.append(this.voltage);

        return out.toString();
    }

    public String toJsonString() {
        if (jsonConverter == null)
            jsonConverter = new Gson();

        return jsonConverter.toJson(this);
    }

    public String getTestName() {
        return testName;
    }

    public long getDataPoint() {
        return this.dataPoint;
    }
}
