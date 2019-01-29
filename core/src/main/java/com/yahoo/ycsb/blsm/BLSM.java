package com.yahoo.ycsb.blsm;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.yahoo.mapkeeper.MapKeeper;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;

public class BLSM extends DB {

    private static final String DB_NAME = "db";

    private static final String URL = "nobelium.ics.uci.edu";

    private static final Timer timer = new Timer();

    static {
        timer.schedule(new TimerTask() {
            private long previousRecord = 0;
            private long counter = 0;

            @Override
            public void run() {
                long totalRecords = BLSM.operationCount.get();
                long ingestedRecords = totalRecords - previousRecord;
                System.out.println(counter + "," + ingestedRecords + "," + totalRecords);
                counter++;
                previousRecord = totalRecords;
            }
        }, 0, 1000);
    }

    public static void main(String[] args) throws TException {
        TSocket socket = new TSocket(URL, 9090);
        TTransport trans = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(trans);
        MapKeeper.Client client = new MapKeeper.Client(protocol);
        trans.open();

        if (args.length != 1) {
            System.out.println("Usage: init, shutdown");
            return;
        }
        if (args[0].equals("init")) {
            client.dropMap(DB_NAME);
            client.addMap(DB_NAME);
            System.out.println("Initialized bLSM");
        } else if (args[0].equals("shutdown")) {
            client.shutdown();
            System.out.println("Shutdown bLSM");
        } else {
            System.out.println("Unknown command " + args[0]);
        }
    }

    private final MapKeeper.Client client;

    private static AtomicLong operationCount = new AtomicLong(0);

    public BLSM() {
        TSocket socket = new TSocket(URL, 9090);
        TTransport trans = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(trans);
        client = new MapKeeper.Client(protocol);
        try {
            trans.open();
        } catch (TTransportException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {
        return Status.OK;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        try {
            ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes());
            ByteArrayOutputStream valueOut = new ByteArrayOutputStream();
            for (String value : values.keySet()) {
                valueOut.write(value.getBytes());
                valueOut.write(values.get(value).toArray());
            }
            ByteBuffer valueBuffer = ByteBuffer.wrap(valueOut.toByteArray());
            client.put(DB_NAME, keyBuffer, valueBuffer);

            operationCount.incrementAndGet();
            return Status.OK;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return update(table, key, values);
    }

    @Override
    public Status delete(String table, String key) {
        return Status.OK;
    }
}
