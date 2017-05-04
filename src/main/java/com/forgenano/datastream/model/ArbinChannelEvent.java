package com.forgenano.datastream.model;

import com.google.gson.Gson;
import joptsimple.internal.Strings;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by michael on 4/7/17.
 */
public class ArbinChannelEvent {

    public static class ArbinCycleStatistics {
        private double cycleVMax;
        private double chargeTime;
        private double dischargeTime;

        public ArbinCycleStatistics(double cycleVMaxRaw, double chargeTimeRaw, double dischargeTimeRaw) {
            this.cycleVMax = cycleVMaxRaw;
            this.chargeTime = chargeTimeRaw;
            this.dischargeTime = dischargeTimeRaw;
        }
    }

    public static class ArbinTestMetadata {

        private static final Logger log = LoggerFactory.getLogger(ArbinTestMetadata.class);

        private static Pattern COMMENTS_WEIGHT_REGEX = Pattern.compile(".*[wW]:\\s*(\\d+(?:\\.?\\d+)?)");
        private static Pattern COMMENTS_RAW_MAT_ID_REGEX = Pattern.compile(".*[mM]_[iI][dD]:\\s*([a-zA-Z_0-9-.]+)");
        private static Pattern COMMENTS_BATCH_ID_REGEX = Pattern.compile(".*[bB]_[iI][dD]:\\s*([a-zA-Z_0-9-.]+)");

        public static ArbinTestMetadata FromCommentsString(String comments, String test_name) {
            if (Strings.isNullOrEmpty(comments))
                return null;

            String weightStr = null;
            double weightValue = -1;
            String rawMaterialId = null;
            String batchId = null;

            try {
                Matcher weightMatcher = COMMENTS_WEIGHT_REGEX.matcher(comments);
                if (weightMatcher.find()) {
                    weightStr = weightMatcher.group(1);

                    if (!Strings.isNullOrEmpty(weightStr)) {
                        try {
                            weightValue = Double.parseDouble(weightStr);
                        }
                        catch(Exception e) {
                            log.error("Failed to convert arbin test metadata, weight value: " + weightStr +
                                    " to a double in test: " + test_name);
                        }
                    }
                }
                else {
                    log.warn("Missing weight in comments for test_name: " + test_name);
                    return null;
                }

                Matcher rawMatIdMatcher = COMMENTS_RAW_MAT_ID_REGEX.matcher(comments);
                if (rawMatIdMatcher.find()) {
                    rawMaterialId = rawMatIdMatcher.group(1);
                }

                Matcher batchIdMatcher = COMMENTS_BATCH_ID_REGEX.matcher(comments);
                if (batchIdMatcher.find()) {
                    batchId = batchIdMatcher.group(1);
                }

                return new ArbinTestMetadata(weightValue, rawMaterialId, batchId);
            }
            catch(Exception e) {
                log.error("Failed to pull the Arbin Test Metadata (test: " + test_name + ") from the comments string: ("
                        + comments + ") because: ", e);

                return null;
            }
        }

        private double materialWeight;
        private String rawMaterialId;
        private String batchId;

        private ArbinTestMetadata(double materialWeight, String rawMaterialId, String batchId) {
            this.materialWeight = materialWeight;
            this.rawMaterialId = rawMaterialId;
            this.batchId = batchId;
        }
    }

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
    private String comments;

    private ArbinCycleStatistics cycleStatistics;
    private ArbinTestMetadata testMetadata;

    public ArbinChannelEvent(long testId, String channelTestName, long dataPoint, int channelIndex, int daqIndex,
                             int channelType, String creator, double testTimeRaw, double stepTimeRaw, double dateTimeRaw,
                             int stepIndex, int cycleIndex, boolean isFCData, double current, double voltage,
                             double chargeCapacity, double dischargeCapacity, double dV_dt, double internalResistance,
                             double acImpedance, double aciPhaseAngle, String scheduleFileName, String comments,
                             ArbinCycleStatistics cycleStatistics) {

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
        this.dV_dt = dV_dt;
        this.internalResistance = internalResistance;
        this.acImpedance = acImpedance;
        this.aciPhaseAngle = aciPhaseAngle;
        this.comments = comments;

        long dateTimeMilliseconds = (long) ((dateTimeRaw - 25569) * 86400 * 1000);

        LocalDateTime dateTime = new LocalDateTime(dateTimeMilliseconds, DateTimeZone.getDefault());

        this.dateTime = dateTime.toString(dateTimeFormatter);
        this.cycleStatistics = cycleStatistics;
        this.testMetadata = ArbinTestMetadata.FromCommentsString(this.comments, this.testName);
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
