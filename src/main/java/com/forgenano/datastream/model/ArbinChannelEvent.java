package com.forgenano.datastream.model;

import com.google.common.collect.*;
import com.google.gson.*;
import joptsimple.internal.Strings;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;
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

        public static class Serializer implements JsonSerializer<ArbinTestMetadata> {

            @Override
            public JsonElement serialize(ArbinTestMetadata src, Type typeOfSrc, JsonSerializationContext context) {
                JsonObject metadataObject = new JsonObject();

                if (src.averageAnodeWeight != -1.0) {
                    metadataObject.add("averageAnodeWeight", context.serialize(src.averageAnodeWeight));
                }

                if (src.averageCathodeWeight != -1.0) {
                    metadataObject.add("averageCathodeWeight", context.serialize(src.averageCathodeWeight));
                }

                if (!src.anodeRawMaterialIds.isEmpty()) {
                    metadataObject.add("anodeRawMaterialIds", context.serialize(src.anodeRawMaterialIds));
                }

                if (!src.anodeBatchIds.isEmpty()) {
                    metadataObject.add("anodeBatchIds", context.serialize(src.anodeBatchIds));
                }

                if (!src.cathodeRawMaterialIds.isEmpty()) {
                    metadataObject.add("cathodeRawMaterialIds", context.serialize(src.cathodeRawMaterialIds));
                }

                if (!src.cathodeBatchIds.isEmpty()) {
                    metadataObject.add("cathodeBatchIds", context.serialize(src.cathodeBatchIds));
                }

                return metadataObject;
            }
        }

        // <C|A>_<M|B>_ID=<id>:<weight>,...,<id>:<weight>
        private static Pattern COMMENTS_C_M_ID_WEIGHT_REGEX = Pattern.compile("C_M_ID=([a-zA-Z_0-9-.:,]+)");
        private static Pattern COMMENTS_C_B_ID_WEIGHT_REGEX = Pattern.compile("C_B_ID=([a-zA-Z_0-9-.:,]+)");
        private static Pattern COMMENTS_A_M_ID_WEIGHT_REGEX = Pattern.compile("A_M_ID=([a-zA-Z_0-9-.:,]+)");
        private static Pattern COMMENTS_A_B_ID_WEIGHT_REGEX = Pattern.compile("A_B_ID=([a-zA-Z_0-9-.:,]+)");

        private static Multimap<String, Double> getIdsAndWeightsForRegex(Pattern regex, String commentsField,
                                                                         String testName) {

            Multimap<String, Double> parsedIdsToWeights = HashMultimap.create();

            try {
                Matcher idsAndWeightsMatcher = regex.matcher(commentsField);

                if (idsAndWeightsMatcher.find()) {
                    String weightsAndIdsString = idsAndWeightsMatcher.group(1);

                    if (!Strings.isNullOrEmpty(weightsAndIdsString)) {
                        String[] weightsAndIdPairs = weightsAndIdsString.split(",");

                        for (String weightsAndIdPair : weightsAndIdPairs) {
                            String[] weightAndIdPair = weightsAndIdPair.split(":");
                            String id = weightAndIdPair[0];
                            String weightStr = weightAndIdPair[1];
                            double weightValue = -1;

                            try {
                                weightValue = Double.parseDouble(weightStr);
                                parsedIdsToWeights.put(id, weightValue);
                            }
                            catch (Exception e) {
                                log.error("Comments for test: " + testName + " failed to convert: (" + weightStr +
                                        ") to a double value because: " + e.getMessage());
                            }
                        }
                    }
                }
            }
            catch(Exception e) {
                log.error("Failed to run the ids and weights regex ( " + regex.pattern() + " ) for the test: " +
                        testName + " on the comments field: " + commentsField + " because: " + e.getMessage());
            }

            return parsedIdsToWeights;
        }

        public static ArbinTestMetadata FromCommentsString(String comments, String testName) {
            if (Strings.isNullOrEmpty(comments))
                return null;

            Multimap<String, Double> cathodeMaterialIdWeights =
                    getIdsAndWeightsForRegex(COMMENTS_C_M_ID_WEIGHT_REGEX, comments, testName);

            Multimap<String, Double> cathodeBatchIdWeights =
                    getIdsAndWeightsForRegex(COMMENTS_C_B_ID_WEIGHT_REGEX, comments, testName);

            Multimap<String, Double> anodeMaterialIdWeights =
                    getIdsAndWeightsForRegex(COMMENTS_A_M_ID_WEIGHT_REGEX, comments, testName);

            Multimap<String, Double> anodeBatchIdWeights =
                    getIdsAndWeightsForRegex(COMMENTS_A_B_ID_WEIGHT_REGEX, comments, testName);

            if (cathodeMaterialIdWeights.isEmpty() && cathodeBatchIdWeights.isEmpty() &&
                anodeMaterialIdWeights.isEmpty() && anodeMaterialIdWeights.isEmpty()) {
                log.warn("The test: " + testName + " has a comments field which was not parsed correctly: " + comments);

                return null;
            }

            double averageCathodeWeight = -1.0;
            double averageAnodeWeight = -1.0;

            Collection<Double> allCathodeWeights = Lists.newArrayList(cathodeBatchIdWeights.values());
            allCathodeWeights.addAll(cathodeMaterialIdWeights.values());

            OptionalDouble optionalCathodeAverage =
                    allCathodeWeights.stream().mapToDouble(Double::doubleValue).average();

            if (optionalCathodeAverage.isPresent())
                averageCathodeWeight = optionalCathodeAverage.getAsDouble();

            Collection<Double> allAnodeWeights = Lists.newArrayList(anodeBatchIdWeights.values());
            allAnodeWeights.addAll(anodeMaterialIdWeights.values());

            OptionalDouble optionalAnodeAverage =
                    allAnodeWeights.stream().mapToDouble(Double::doubleValue).average();

            if (optionalAnodeAverage.isPresent())
                averageAnodeWeight = optionalAnodeAverage.getAsDouble();

            return new ArbinTestMetadata(averageCathodeWeight, averageAnodeWeight, cathodeMaterialIdWeights.keySet(),
                    cathodeBatchIdWeights.keySet(), anodeMaterialIdWeights.keySet(), anodeBatchIdWeights.keySet());
        }

        double averageAnodeWeight;
        double averageCathodeWeight;
        Collection<String> cathodeRawMaterialIds;
        Collection<String> cathodeBatchIds;
        Collection<String> anodeRawMaterialIds;
        Collection<String> anodeBatchIds;

        private ArbinTestMetadata(double averageCathodeWeight, double averageAnodeWeight,
                                  Collection<String> cathodeRawMaterialIds, Collection<String> cathodeBatchIds,
                                  Collection<String> anodeRawMaterialIds, Collection<String> anodeBatchIds) {
            this.averageAnodeWeight = averageAnodeWeight;
            this.averageCathodeWeight = averageCathodeWeight;
            this.cathodeRawMaterialIds = cathodeRawMaterialIds;
            this.cathodeBatchIds = cathodeBatchIds;
            this.anodeRawMaterialIds = anodeRawMaterialIds;
            this.anodeBatchIds = anodeBatchIds;
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
        StringBuilder out = new StringBuilder();

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
        if (jsonConverter == null) {

            GsonBuilder gsonBuilder = new GsonBuilder();

            gsonBuilder.registerTypeAdapter(ArbinTestMetadata.class, new ArbinTestMetadata.Serializer());

            jsonConverter = gsonBuilder.create();
        }

        return jsonConverter.toJson(this);
    }

    public String getTestName() {
        return testName;
    }

    public long getDataPoint() {
        return this.dataPoint;
    }
}
