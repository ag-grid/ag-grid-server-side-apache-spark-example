package unit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FileWritingPerfTest {


    private static final int ITERATIONS = 5;
    private static final double MEG = (Math.pow(1024, 2));
    private static final int RECORD_COUNT = 4000000;
    private static final String RECORD = "Help I am trapped in a fortune cookie factory\n";
    private static final int RECSIZE = RECORD.getBytes().length;

    public static void main(String[] args) throws Exception {
        List<String> records = new ArrayList<String>(RECORD_COUNT);
        int size = 0;
        for (int i = 0; i < RECORD_COUNT; i++) {
            records.add(RECORD);
            size += RECSIZE;
        }
        System.out.println(records.size() + " 'records'");
        System.out.println(size / MEG + " MB");

        for (int i = 0; i < ITERATIONS; i++) {
            System.out.println("\nIteration " + i);
            writeBuffered(records, 4 * (int) MEG);
        }
    }


    private static void writeBuffered(List<String> records, int bufSize) throws IOException {
        File writeFile = new File("/Users/robertclarke/Development/projects/scratch/olympic-medals/src/main/resources/data/result1.csv");

        try {
            FileWriter writer = new FileWriter(writeFile);
            BufferedWriter bufferedWriter = new BufferedWriter(writer, bufSize);

            System.out.print("Writing buffered (buffer size: " + bufSize + ")... ");
            write(records, bufferedWriter);
        } finally {
            // comment this out if you want to inspect the files afterward
//            file.delete();
        }
    }

    private static void write(List<String> records, Writer writer) throws IOException {
        long start = System.currentTimeMillis();
        for (String record: records) {
            writer.write(record);
        }
        writer.flush();
        writer.close();
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000f + " seconds");
    }
}