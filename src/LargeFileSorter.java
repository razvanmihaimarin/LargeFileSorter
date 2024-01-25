import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Simple file sorter for large input files containing data in the following format:
 * <p>
 * Id1<tab>date1<tab>data\r\n <p>
 * Id1<tab>date2<tab>data\r\n <p>
 * Id2<tab>date1<tab>data\r\n <p>
 * Id2<tab>date2<tab>data\r\n <p>
 * Sorts the lines in the new file by id and then by date.
 */
public class LargeFileSorter {
    public static final String FILE_EXTENSION = ".txt";
    public static final String TAB = "\t";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            if (args.length != 2) {
                System.out.println("Usage: java LargeFileSorter inputFilePath outputFilePath");
                System.out.println("Please provide two arguments.");
                System.exit(1); // Exit with an error code
            }

            // Extract and use the provided arguments
            String inputFile = args[0];
            String outputFile = args[1];

            File file = new File(inputFile);
            //checks if the specified exists or the specified file is a normal file (not a directory)
            if (!file.exists() || !file.isFile()) {
                System.out.println("Input file does not exist.");
                return;
            }
            System.out.println("Starting sorting algorithm.");
            // run sorting
            externalSort(inputFile, outputFile);
            System.out.println("Sorting completed. Output file: " + outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        long elapsedSeconds = elapsedTime / 1000;
        System.out.println("Elapsed time: " + elapsedSeconds + " seconds");
    }

    /**
     * Creates final sorted file(output file) by id and timestamp
     *
     * @param inputFile  The unsorted input file
     * @param outputFile The sorted output file
     * @throws IOException
     */
    private static void externalSort(String inputFile, String outputFile) throws IOException {
        List<Path> temporaryFiles = explodeToTemporaryFiles(inputFile);
        System.out.println("Exploded files created successfully.");
        List<Path> temporarySortedFiles = new ArrayList<>();
        IntStream.range(0, temporaryFiles.size()).forEach((value) -> {
            sortFileContents(temporaryFiles.get(value), value);
            try {
                Files.delete(temporaryFiles.get(value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            temporarySortedFiles.add(Paths.get(getTempSortedFileName(value)));
        });
        System.out.println("Sorted files created successfully.");
        mergeSortedFiles(temporarySortedFiles, outputFile);
    }

    /**
     * Creates temporary files by splitting the initial file into smaller,
     * 100 MB files
     *
     * @param inputFile The input file
     * @return List of Paths indicating the newly created files
     */
    private static List<Path> explodeToTemporaryFiles(String inputFile) {
        BufferedReader reader;
        List<Path> tempFileNames = new ArrayList<>();
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            String line = reader.readLine();
            int i = 0;
            double totalSize = 0;
            String fileName = getTempFileName(i);
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            while (line != null) {
                writer.write(line);
                writer.newLine();
                final byte[] byteArray = line.getBytes(StandardCharsets.UTF_8);
                totalSize += (double) byteArray.length / 1024 / 1024;
                // max 100 MB file size
                if (totalSize > 100) {
                    writer.close();
                    tempFileNames.add(Paths.get(fileName));
                    i++;
                    fileName = getTempFileName(i);
                    writer = new BufferedWriter(new FileWriter(fileName));
                    totalSize = 0;
                }
                line = reader.readLine();
            }
            writer.close();
            tempFileNames.add(Paths.get(fileName));
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tempFileNames;
    }

    /**
     * For a given file, represented by a Path object, it sorts its contents
     * by id, then by timestamp
     *
     * @param filename The Path object painting towards the file that will be sorted
     * @param index    The file index
     */
    private static void sortFileContents(final Path filename, final int index) {
        try (Stream<String> lines = Files.lines(filename); PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get(getTempSortedFileName(index))))) {
            lines.sorted(Comparator.comparing(LargeFileSorter::getId).thenComparingLong(LargeFileSorter::getTimestamp)).forEach(pw::println);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error sorting file with name: %s", filename), e);
        }
    }

    /**
     * Creates the final output file by merging the sorted, smaller files
     *
     * @param sortedFiles List of Paths indicating the sorted files
     * @param outputFile  The full path of the output file
     * @throws IOException
     */
    private static void mergeSortedFiles(List<Path> sortedFiles, String outputFile) throws IOException {
        List<BufferedReader> readers = sortedFiles.stream().map(path -> {
            try {
                return Files.newBufferedReader(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).toList();

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {
            PriorityQueue<CustomLine> priorityQueue = new PriorityQueue<>(Comparator.comparing(CustomLine::getId).thenComparingLong(CustomLine::getTimestamp));

            for (BufferedReader reader : readers) {
                String line = reader.readLine();
                if (line != null) {
                    CustomLine customLine = new CustomLine(line, reader);
                    priorityQueue.add(customLine);
                }
            }

            while (!priorityQueue.isEmpty()) {
                CustomLine customLine = priorityQueue.poll();
                writer.write(customLine.getLine());
                writer.newLine();

                String nextLine = customLine.getReader().readLine();
                if (nextLine != null) {
                    CustomLine nextCustomLine = new CustomLine(nextLine, customLine.getReader());
                    priorityQueue.add(nextCustomLine);
                }
            }
        }

        // Clean up temporary sorted files
        for (Path file : sortedFiles) {
            Files.delete(file);
        }
    }

    private static String getTempFileName(int i) {
        return "tempFile" + i + FILE_EXTENSION;
    }

    private static String getTempSortedFileName(int i) {
        return "tempFileSorted" + i + FILE_EXTENSION;
    }

    private static String getId(String line) {
        return line.split(TAB)[0];
    }

    private static long getTimestamp(String line) {
        return Long.parseLong(line.split(TAB)[1]);
    }

    private static class CustomLine {
        private final String line;
        private final BufferedReader reader;
        private final String id;
        private final long timestamp;

        public CustomLine(String line, BufferedReader reader) {
            this.line = line;
            this.reader = reader;
            this.id = LargeFileSorter.getId(line);
            this.timestamp = LargeFileSorter.getTimestamp(line);
        }

        public String getLine() {
            return line;
        }

        public BufferedReader getReader() {
            return reader;
        }

        public String getId() {
            return id;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
