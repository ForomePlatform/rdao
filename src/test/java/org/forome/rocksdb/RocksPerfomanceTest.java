package org.forome.rocksdb;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.descriptive.rank.Median;
import org.junit.Assert;
import org.rocksdb.*;

import javax.swing.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;

public class RocksPerfomanceTest {

    private static class ByteUtils {
        private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

        public static byte[] longToBytes(long x) {
            buffer.putLong(0, x);
            return buffer.array();
        }

        public static long bytesToLong(byte[] bytes) {
            buffer.put(bytes, 0, bytes.length);
            buffer.flip();//need flip
            return buffer.getLong();
        }
    }

    private static String PROCESS_PID = "-";
    static {
        try {
            String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
            if (processName != null && processName.length() > 0) {
                String[] parse = processName.split("@");
                PROCESS_PID = parse[0];
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void showMessage(String message) {
        JOptionPane optionPane = new JOptionPane(message);
        JDialog dialog = optionPane.createDialog("Perfomance test");
        dialog.setAlwaysOnTop(true);
        dialog.setVisible(true);
    }

    //@Test
    public void writingDataTest() throws RocksDBException, IOException {
        Path pathDataBase = Files.createTempDirectory("rocksdb");
        pathDataBase.toAbsolutePath().toFile().deleteOnExit();

        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString());
            final WriteOptions writeOptions = new WriteOptions();
            final ReadOptions readOptions = new ReadOptions()) {

            System.out.println(PROCESS_PID);

            showMessage("Test on starting...");

            final long totalSize = 2L * 1024L * 1024L * 1024L;
            final long packageSize = 50L * 1024L * 1024L;
            final int valueSize = 1024;
            final int keySize = 100;

            final SecureRandom secureRandom = new SecureRandom();

            long totalCommitedSize = 0;
            while (totalCommitedSize < totalSize) {
                try (final Transaction txn = txnDb.beginTransaction(writeOptions)) {
                    long commitedSize = 0;
                    while (commitedSize < packageSize) {
                        byte[] key = secureRandom.generateSeed(keySize);
                        byte[] value = secureRandom.generateSeed(valueSize);
                        txn.put(key, value);
                        //txnDb.put(key, value);
                        commitedSize += valueSize;
                    }
                    txn.commit();
                    totalCommitedSize += commitedSize;
                }
            }

            showMessage("Test on stopped.");
        }

        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }

    //@Test
    public void gettingDataTest() throws RocksDBException, IOException {
        final int VALUE_SIZE = 512;
        final int BEGIN = 1000000;
        final int END = 2000000;
        final int STEP = 100000;
        final int FINDING_COUNT = 10000;

        final SecureRandom secureRandom = new SecureRandom();
        for (int keyCount = BEGIN; keyCount <= END; keyCount += STEP) {
            Path pathDataBase = Files.createTempDirectory("rocksdb");
            pathDataBase.toAbsolutePath().toFile().deleteOnExit();

            try (final Options options = new Options().setCreateIfMissing(true);
                 final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString())) {

                for (long i = 1; i <= keyCount; i++) {
                    byte[] key = ByteUtils.longToBytes(i);
                    byte[] value = secureRandom.generateSeed(VALUE_SIZE);
                    txnDb.put(key, value);
                }

                int step = keyCount / FINDING_COUNT;
                if (step == 0) {
                    step = 1;
                }

                int pointCount = keyCount / step;
                double[] times = new double[pointCount];
                for (int i = 0; i < pointCount; i++) {
                    byte[] key = ByteUtils.longToBytes(i * step + 1);
                    long time = System.nanoTime();
                    byte[] value = txnDb.get(key);
                    times[i] = System.nanoTime() - time;
                    Assert.assertNotNull(value);
                }
                double mediana = (new Median()).evaluate(times);
                System.out.println(Long.toString(keyCount) + "\t" + (long) mediana);
            }

            FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
        }
    }
}
