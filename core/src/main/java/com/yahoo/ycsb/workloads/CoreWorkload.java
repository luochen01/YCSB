/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.AcknowledgedCounterGenerator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SequentialGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformLongGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>minfieldlength</b>: the minimum size of each field (default: 1)scanlengthdistribution
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
 * modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
 * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
 * <LI><b>minscanlength</b>: for scans, what is the minimum number of records to scan (default: 1)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
 * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
 * YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
 * YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
 * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
 * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
 * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
 * digits in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
 * order ("hashed") (default: hashed)
 * <LI><b>fieldnameprefix</b>: what should be a prefix for field names, the shorter may decrease the
 * required storage size (default: "field")
 * </ul>
 */
public class CoreWorkload extends Workload {
    /**
     * The name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY = "table";

    /**
     * The default name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY_DEFAULT = "ds";

    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY = "fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

    private String tablePrefix;

    private List<String> fieldnames;

    /**
     * The name of the property for the field length distribution. Options are "uniform", "zipfian"
     * (favouring short records), "constant", and "histogram".
     * <p>
     * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
     * fieldlength property. If "histogram", then the histogram will be read from the filename
     * specified in the "fieldlengthhistogram" property.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

    /**
     * The default field length distribution.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

    /**
     * The name of the property for the length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

    /**
     * The default maximum length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

    /**
     * The name of the property for the minimum length of a field in bytes.
     */
    public static final String MIN_FIELD_LENGTH_PROPERTY = "minfieldlength";

    /**
     * The default minimum length of a field in bytes.
     */
    public static final String MIN_FIELD_LENGTH_PROPERTY_DEFAULT = "1";

    /**
     * The name of a property that specifies the filename containing the field length histogram (only
     * used if fieldlengthdistribution is "histogram").
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

    /**
     * The default filename containing a field length histogram.
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

    /**
     * Generator object that produces field lengths. The value of this depends on the properties that
     * start with "FIELD_LENGTH_".
     */
    protected NumberGenerator fieldlengthgenerator;

    /**
     * The name of the property for deciding whether to read one field (false) or all fields (true) of
     * a record.
     */
    public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

    /**
     * The default value for the readallfields property.
     */
    public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

    protected boolean readallfields;

    /**
     * The name of the property for deciding whether to write one field (false) or all fields (true)
     * of a record.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

    /**
     * The default value for the writeallfields property.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

    protected boolean writeallfields;

    /**
     * The name of the property for deciding whether to check all returned
     * data against the formation template to ensure data integrity.
     */
    public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

    /**
     * The default value for the dataintegrity property.
     */
    public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "true";

    /**
     * Set to true if want to check correctness of reads. Must also
     * be set to true during loading phase to function.
     */
    private boolean dataintegrity;

    /**
     * The name of the property for the proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

    /**
     * The name of the property for the proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

    /**
     * The default proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

    /**
     * The name of the property for the proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are read-modify-write.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the the distribution of requests across the keyspace. Options are
     * "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

    /**
     * The default distribution of requests across the keyspace.
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for adding zero padding to record numbers in order to match
     * string sort order. Controls the number of 0s to left pad with.
     */
    public static final String ZERO_PADDING_PROPERTY = "zeropadding";

    /**
     * The default zero padding value. Matches integer sort order
     */
    public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";

    /**
     * The name of the property for the min scan length (number of records).
     */
    public static final String MIN_SCAN_LENGTH_PROPERTY = "minscanlength";

    /**
     * The default min scan length.
     */
    public static final String MIN_SCAN_LENGTH_PROPERTY_DEFAULT = "1";

    /**
     * The name of the property for the max scan length (number of records).
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

    /**
     * The default max scan length.
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

    /**
     * The name of the property for the scan length distribution. Options are "uniform" and "zipfian"
     * (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

    /**
     * The default max scan length.
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the order to insert records. Options are "ordered" or "hashed"
     */
    public static final String INSERT_ORDER_PROPERTY = "insertorder";

    /**
     * Default insert order.
     */
    public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

    /**
     * Percentage data items that constitute the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

    /**
     * Default value of the size of the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    /**
     * Percentage operations that access the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

    /**
     * Default value of the percentage operations accessing the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

    /**
     * How many times to retry when insertion of a single item to a DB fails.
     */
    public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
    public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

    /**
     * On average, how long to wait between the retries, in seconds.
     */
    public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
    public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

    /**
     * On average, how long to wait between the retries, in seconds.
     */
    public static final String TABLE_COUNT_PROPERTY = "tablecount";
    public static final String TABLE_COUNT_PROPERTY_DEFAULT = "1";

    public static final String TABLE_OPERATION_PORTION = "table.operation.portion";
    public static final String TABLE_OPERATION_PORTION_DEFAULT = "1";

    public static final String TABLE_RECORD_PORTION = "table.record.portion";
    public static final String TABLE_RECORD_PORTION_DEFAULT = "1";

    /**
     * Field name prefix.
     */
    public static final String FIELD_NAME_PREFIX = "fieldnameprefix";

    /**
     * Default value of the field name prefix.
     */
    public static final String FIELD_NAME_PREFIX_DEFAULT = "field";

    protected NumberGenerator[] keysequence;
    protected DiscreteGenerator<String> operationchooser;
    protected DiscreteGenerator<Integer> tablechooser;
    protected NumberGenerator[] keychooser;
    protected NumberGenerator fieldchooser;
    protected AcknowledgedCounterGenerator[] transactioninsertkeysequence;
    protected NumberGenerator scanlength;
    protected boolean orderedinserts;
    protected long fieldcount;
    protected int tablecount;
    protected long recordcount;
    protected long[] tablerecordcount;
    protected int zeropadding;
    protected int insertionRetryLimit;
    protected int insertionRetryInterval;
    protected String[] tablenames;

    private Measurements measurements = Measurements.getMeasurements();

    protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
        NumberGenerator fieldlengthgenerator;
        String fieldlengthdistribution =
                p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
        int minfieldlength =
                Integer.parseInt(p.getProperty(MIN_FIELD_LENGTH_PROPERTY, MIN_FIELD_LENGTH_PROPERTY_DEFAULT));
        String fieldlengthhistogram =
                p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (fieldlengthdistribution.compareTo("constant") == 0) {
            fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
        } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
            fieldlengthgenerator = new UniformLongGenerator(minfieldlength, fieldlength);
        } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
            fieldlengthgenerator = new ZipfianGenerator(minfieldlength, fieldlength);
        } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
            try {
                fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
            } catch (IOException e) {
                throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
            }
        } else {
            throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
        }
        return fieldlengthgenerator;
    }

    /**
     * Initialize the scenario.
     * Called once, in the main client thread, before any operations are started.
     */
    @Override
    public void init(Properties p) throws WorkloadException {
        tablePrefix = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

        fieldcount = Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
        final String fieldnameprefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
        fieldnames = new ArrayList<>();
        for (int i = 0; i < fieldcount; i++) {
            fieldnames.add(fieldnameprefix + i);
        }
        fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

        tablecount = Integer.parseInt(p.getProperty(TABLE_COUNT_PROPERTY, TABLE_COUNT_PROPERTY_DEFAULT));
        recordcount = Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

        tablerecordcount = new long[tablecount];
        String[] strs = p.getProperty(TABLE_RECORD_PORTION, TABLE_RECORD_PORTION_DEFAULT).split(",");
        if (tablecount != strs.length) {
            System.err.println("Illegal value of " + TABLE_RECORD_PORTION + ". Expecting " + tablecount + " tables.");
            System.exit(-1);
        }
        tablenames = new String[tablecount];
        for (int i = 0; i < tablecount; i++) {
            tablerecordcount[i] = (long) (Double.valueOf(strs[i]) * recordcount);
            tablenames[i] = tablePrefix + i;
        }

        String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int minscanlength = Integer.parseInt(p.getProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_PROPERTY_DEFAULT));
        int maxscanlength = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
        String scanlengthdistrib =
                p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

        long insertstart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
        long insertcount =
                Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
        // Confirm valid values for insertstart and insertcount in relation to recordcount
        if (recordcount < (insertstart + insertcount)) {
            System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
            System.err.println("recordcount must be bigger than insertstart + insertcount.");
            System.exit(-1);
        }
        zeropadding = Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

        readallfields = Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
        writeallfields =
                Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

        dataintegrity = Boolean.parseBoolean(p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
        // Confirm that fieldlengthgenerator returns a constant if data
        // integrity check requested.
        if (dataintegrity
                && !(p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT))
                        .equals("constant")) {
            System.err.println("Must have constant field size to check data integrity.");
            System.exit(-1);
        }
        keychooser = new NumberGenerator[tablecount];

        if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
            orderedinserts = false;
        } else if (requestdistrib.compareTo("exponential") == 0) {
            double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
            double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
            for (int i = 0; i < tablecount; i++) {
                keychooser[i] = new ExponentialGenerator(percentile, tablerecordcount[i] * frac);
            }
        } else {
            orderedinserts = true;
        }

        keysequence = new NumberGenerator[tablecount];
        for (int i = 0; i < tablecount; i++) {
            keysequence[i] = new CounterGenerator(insertstart);
        }
        operationchooser = createOperationGenerator(p);
        tablechooser = createTableGenerator(p);

        transactioninsertkeysequence = new AcknowledgedCounterGenerator[tablecount];
        for (int i = 0; i < tablecount; i++) {
            transactioninsertkeysequence[i] = new AcknowledgedCounterGenerator(tablerecordcount[i]);
        }

        if (requestdistrib.compareTo("uniform") == 0) {
            for (int i = 0; i < tablecount; i++) {
                keychooser[i] = new UniformLongGenerator(insertstart, insertstart + tablerecordcount[i] - 1);
            }
        } else if (requestdistrib.compareTo("sequential") == 0) {
            for (int i = 0; i < tablecount; i++) {
                keychooser[i] = new SequentialGenerator(insertstart, insertstart + tablerecordcount[i] - 1);
            }
        } else if (requestdistrib.compareTo("zipfian") == 0) {
            // it does this by generating a random "next key" in part by taking the modulus over the
            // number of keys.
            // If the number of keys changes, this would shift the modulus, and we don't want that to
            // change which keys are popular so we'll actually construct the scrambled zipfian generator
            // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
            // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
            // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
            // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
            // the keyspace doesn't change from the perspective of the scrambled zipfian generator
            for (int i = 0; i < tablecount; i++) {
                final double insertproportion = Double
                        .parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
                int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
                int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

                keychooser[i] =
                        new ScrambledZipfianGenerator(insertstart, insertstart + tablerecordcount[i] + expectednewkeys);
            }
        } else if (requestdistrib.compareTo("latest") == 0) {
            for (int i = 0; i < tablecount; i++) {
                keychooser[i] = new SkewedLatestGenerator(transactioninsertkeysequence[i]);
            }
        } else if (requestdistrib.equals("hotspot")) {
            double hotsetfraction =
                    Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
            double hotopnfraction =
                    Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
            for (int i = 0; i < tablecount; i++) {
                keychooser[i] = new HotspotIntegerGenerator(insertstart, insertstart + tablerecordcount[i] - 1,
                        hotsetfraction, hotopnfraction);
            }
        } else {
            throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
        }

        fieldchooser = new UniformLongGenerator(0, fieldcount - 1);

        if (scanlengthdistrib.compareTo("uniform") == 0) {
            scanlength = new UniformLongGenerator(minscanlength, maxscanlength);
        } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
            scanlength = new ZipfianGenerator(minscanlength, maxscanlength);
        } else {
            throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
        }

        insertionRetryLimit = Integer.parseInt(p.getProperty(INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
        insertionRetryInterval =
                Integer.parseInt(p.getProperty(INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
    }

    protected String buildKeyName(long keynum) {
        if (!orderedinserts) {
            keynum = Utils.hash(keynum);
        }
        String value = Long.toString(keynum);
        int fill = zeropadding - value.length();
        String prekey = "user";
        for (int i = 0; i < fill; i++) {
            prekey += '0';
        }
        return prekey + value;
    }

    /**
     * Builds a value for a randomly chosen field.
     */
    private HashMap<String, ByteIterator> buildSingleValue(String key, int table, boolean insert) {
        HashMap<String, ByteIterator> value = new HashMap<>();

        String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
        ByteIterator data;
        if (dataintegrity) {
            data = new StringByteIterator(buildDeterministicValue(key, fieldkey, table, insert));
        } else {
            // fill with random data
            data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
        }
        value.put(fieldkey, data);

        return value;
    }

    /**
     * Builds values for all fields.
     */
    private HashMap<String, ByteIterator> buildValues(String key, int table, boolean insert) {
        HashMap<String, ByteIterator> values = new HashMap<>();

        for (String fieldkey : fieldnames) {
            ByteIterator data;
            if (dataintegrity) {
                data = new StringByteIterator(buildDeterministicValue(key, fieldkey, table, insert));
            } else {
                // fill with random data
                data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
            }
            values.put(fieldkey, data);
        }
        return values;
    }

    /**
     * Build a deterministic value given the key information.
     */
    private String buildDeterministicValue(String key, String fieldkey, int table, boolean insert) {
        int size = fieldlengthgenerator.nextValue().intValue();
        StringBuilder sb = new StringBuilder(size);
        //sb.append(key);
        //sb.append(':');
        sb.append(fieldkey);
        sb.append(':');

        long skey = insert ? keysequence[table].lastValue().longValue() : nextKeynum(table);
        if (!orderedinserts) {
            skey = Utils.hash(skey);
        }
        String keynumStr = String.valueOf(skey);
        int padding = size - keynumStr.length() - sb.length();
        for (int i = 0; i < padding; i++) {
            sb.append('0');
        }
        sb.append(keynumStr);
        sb.setLength(size);
        return sb.toString();
    }

    private long tablecountsum = -1;
    private long insertcount = 0;
    private int currentTable = -1;

    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads,
     * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
     * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
     * have no side effects other than DB operations.
     */
    @Override
    public boolean doInsert(DB db, Object threadstate) {
        if (currentTable < 0 || (insertcount > tablecountsum && currentTable + 1 < tablecount)) {
            currentTable++;
            tablecountsum += tablerecordcount[currentTable];
        }
        insertcount++;
        Status status;
        int numOfRetries = 0;
        long keynum = keysequence[currentTable].nextValue().longValue();
        String dbkey = buildKeyName(keynum);
        HashMap<String, ByteIterator> values = buildValues(dbkey, currentTable, true);
        do {
            status = db.insert(tablenames[currentTable], dbkey, values);
            if (null != status && status.isOk()) {
                break;
            }
            // Retry if configured. Without retrying, the load process will fail
            // even if one single insertion fails. User can optionally configure
            // an insertion retry limit (default is 0) to enable retry.
            if (++numOfRetries <= insertionRetryLimit) {
                System.err.println("Retrying insertion, retry count: " + numOfRetries);
                try {
                    // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
                    int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    break;
                }

            } else {
                System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries
                        + "Insertion Retry Limit: " + insertionRetryLimit);
                break;

            }
        } while (true);

        return null != status && status.isOk();
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from multiple client
     * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
     * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
     * have no side effects other than DB operations.
     */
    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        String operation = operationchooser.nextString();
        if (operation == null) {
            return false;
        }

        int table = tablechooser.nextValue();
        switch (operation) {
            case "READ":
                doTransactionRead(db, table);
                break;
            case "UPDATE":
                doTransactionUpdate(db, table, threadstate);
                break;
            case "INSERT":
                doTransactionInsert(db, table);
                break;
            case "SCAN":
                doTransactionScan(db, table);
                break;
            default:
                doTransactionReadModifyWrite(db, table);
        }

        return true;
    }

    /**
     * Results are reported in the first three buckets of the histogram under
     * the label "VERIFY".
     * Bucket 0 means the expected data was returned.
     * Bucket 1 means incorrect data was returned.
     * Bucket 2 means null data was returned when some data was expected.
     */
    protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
        Status verifyStatus = Status.OK;
        //        long startTime = System.nanoTime();
        //        if (!cells.isEmpty()) {
        //            for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        //                if (!entry.getValue().toString().equals(buildDeterministicValue(0, key, entry.getKey()))) {
        //                    verifyStatus = Status.UNEXPECTED_STATE;
        //                    break;
        //                }
        //            }
        //        } else {
        //            // This assumes that null data is never valid
        //            verifyStatus = Status.ERROR;
        //        }
        //        long endTime = System.nanoTime();
        //        measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
        //        measurements.reportStatus("VERIFY", verifyStatus);
    }

    long nextKeynum(int table) {
        long keynum;
        if (keychooser[table] instanceof ExponentialGenerator) {
            do {
                keynum = transactioninsertkeysequence[table].lastValue() - keychooser[table].nextValue().intValue();
            } while (keynum < 0);
        } else {
            do {
                keynum = keychooser[table].nextValue().intValue();
            } while (keynum > transactioninsertkeysequence[table].lastValue());
        }
        return keynum;
    }

    public void doTransactionRead(DB db, int table) {
        // choose a random key
        long keynum = nextKeynum(table);

        String keyname = buildKeyName(keynum);

        HashSet<String> fields = null;

        if (!readallfields) {
            // read a random field
            String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

            fields = new HashSet<String>();
            fields.add(fieldname);
        } else if (dataintegrity) {
            // pass the full field list if dataintegrity is on for verification
            fields = new HashSet<String>(fieldnames);
        }

        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
        db.read(tablenames[table], keyname, fields, cells);

        if (dataintegrity) {
            verifyRow(keyname, cells);
        }
    }

    public void doTransactionReadModifyWrite(DB db, int table) {
        // choose a random key
        long keynum = nextKeynum(table);

        String keyname = buildKeyName(keynum);

        HashSet<String> fields = null;

        if (!readallfields) {
            // read a random field
            String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        HashMap<String, ByteIterator> values;

        if (writeallfields) {
            // new data for all the fields
            values = buildValues(keyname, table, false);
        } else {
            // update a random field
            values = buildSingleValue(keyname, table, false);
        }

        // do the transaction

        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();

        long ist = measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        db.read(tablenames[table], keyname, fields, cells);

        db.update(tablenames[table], keyname, values);

        long en = System.nanoTime();

        if (dataintegrity) {
            verifyRow(keyname, cells);
        }

        measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
        measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
    }

    public void doTransactionScan(DB db, int table) {
        // choose a random key
        long keynum = nextKeynum(table);

        String startkeyname = buildKeyName(keynum);

        // choose a random scan length
        int len = scanlength.nextValue().intValue();

        HashSet<String> fields = null;

        if (!readallfields) {
            // read a random field
            String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        db.scan(tablenames[table], startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
    }

    public void doTransactionUpdate(DB db, int table, Object state) {
        // choose a random key
        long keynum = nextKeynum(table);

        String keyname = buildKeyName(keynum);

        HashMap<String, ByteIterator> values;

        if (writeallfields) {
            // new data for all the fields
            values = buildValues(keyname, table, false);
        } else {
            // update a random field
            values = buildSingleValue(keyname, table, false);
        }

        db.update(tablenames[table], keyname, values);
    }

    public void doTransactionInsert(DB db, int table) {
        // choose the next key
        long keynum = transactioninsertkeysequence[table].nextValue();

        try {
            String dbkey = buildKeyName(keynum);

            HashMap<String, ByteIterator> values = buildValues(dbkey, table, false);
            db.insert(tablenames[table], dbkey, values);
        } finally {
            transactioninsertkeysequence[table].acknowledge(keynum);
        }
    }

    /**
     * Creates a weighted discrete values with database operations for a workload to perform.
     * Weights/proportions are read from the properties list and defaults are used
     * when values are not configured.
     * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
     *
     * @param p
     *            The properties list to pull weights from.
     * @return A generator that can be used to determine the next operation to perform.
     * @throws IllegalArgumentException
     *             if the properties object was null.
     */
    protected static DiscreteGenerator<String> createOperationGenerator(final Properties p) {
        if (p == null) {
            throw new IllegalArgumentException("Properties object cannot be null");
        }
        final double readproportion =
                Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        final double updateproportion =
                Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        final double insertproportion =
                Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        final double scanproportion =
                Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
        final double readmodifywriteproportion = Double.parseDouble(
                p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

        final DiscreteGenerator<String> operationchooser = new DiscreteGenerator<>();
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (updateproportion > 0) {
            operationchooser.addValue(updateproportion, "UPDATE");
        }

        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        if (scanproportion > 0) {
            operationchooser.addValue(scanproportion, "SCAN");
        }

        if (readmodifywriteproportion > 0) {
            operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
        }
        return operationchooser;
    }

    protected DiscreteGenerator<Integer> createTableGenerator(final Properties p) {
        if (p == null) {
            throw new IllegalArgumentException("Properties object cannot be null");
        }
        String[] strs = p.getProperty(TABLE_OPERATION_PORTION, TABLE_COUNT_PROPERTY_DEFAULT).split(",");
        if (strs.length != tablecount) {
            System.err.println(
                    "Illegal configuration of " + TABLE_OPERATION_PORTION + ". Expecting " + tablecount + " tables.");
            System.exit(-1);
        }

        DiscreteGenerator<Integer> tablechooser = new DiscreteGenerator<>();
        for (int i = 0; i < tablecount; i++) {
            double portion = Double.valueOf(strs[i]);
            if (portion > 0) {
                tablechooser.addValue(portion, i);
            }
        }
        return tablechooser;
    }

    @Override
    public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
        return null;
    }

}
