package org.pankratzlab.kdmatch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KDMatch {

  // prototype for matching using KD trees(https://en.wikipedia.org/wiki/K-d_tree), with the
  // resolution of duplicate
  // selections via the Hungarian Algorithm (https://en.wikipedia.org/wiki/Hungarian_algorithm)
  // Warning - no real data checks and most things are assumed to be well formed

  // Methods:
  // 1. select k (k greater than final needed) nearest neighbors for all samples
  // 2. prune matches that are completely unique
  // 3. Form groups of samples that share a neighbor
  // 4. Use hungarian algorithm to allocate controls to minimize total distance between cases and
  // controls

  // TODO. optimize duplicates within "communities" only (i.e. only within cases that share at least
  // one control)

  private static void run(Path inputFileAnchor, Path inputFileBarns, Path ouputDir,
                          int initialNumSelect, int finalNumSelect) throws IOException {
    Logger log = Logger.getAnonymousLogger();
    String[] headerA = Files.lines(inputFileAnchor).findFirst().get().toString().trim().split("\t");
    String[] headerB = Files.lines(inputFileBarns).findFirst().get().toString().trim().split("\t");
    new File(ouputDir.toString()).mkdirs();

    if (Arrays.equals(headerA, headerB)) {
      KDTree<Sample> kdTree = new KDTree<Sample>(headerA.length - 1);// dimension of the data to be
                                                                     // searched
      log.info("Assuming 1 ID column and " + (headerA.length - 1) + " data columns");

      log.info("building tree from " + inputFileBarns.toString());
      SelectOptimizedNeighbors.addSamplesToTree(kdTree, getSampleStreamFromFile(inputFileBarns));
      log.info("selecting " + initialNumSelect + " nearest neighbors for "
               + inputFileBarns.toString());
      // The initial selection seems to be quick and scales well (seconds on most data)
      List<Match> matches = SelectOptimizedNeighbors.getNearestNeighborsForSamples(kdTree,
                                                                                   getSampleStreamFromFile(inputFileAnchor),
                                                                                   initialNumSelect)
                                                    .collect(Collectors.toList());
      String outputBase = ouputDir + File.separator + "test.match.AllowDups.txt";

      log.info("reporting full baseline selection of " + initialNumSelect + " nearest neighbors to "
               + outputBase);
      writeToFile(matches.stream(), outputBase, headerA, headerB, initialNumSelect);

      String outputOpt = ouputDir + File.separator + "test.match.optimized.txt";

      // De-duplicating matches does not scale well at all with O(n3)-ish

      log.info("reporting optimized baseline selection of " + finalNumSelect
               + " nearest neighbors to " + outputOpt);
      writeToFile(SelectOptimizedNeighbors.optimizeDuplicates(matches, finalNumSelect, log),
                  outputOpt, headerA, headerB, finalNumSelect);

    }

  }

  private static Stream<Sample> getSampleStreamFromFile(Path inputFileBarns) throws IOException {
    return Files.lines(inputFileBarns).map(l -> l.split("\t")).skip(1)
                .map(s -> new Sample(s[0], Arrays.stream(s).skip(1).mapToDouble(Double::parseDouble)
                                                 .toArray()));
  }

  private static void writeToFile(Stream<Match> matches, String output, String[] headerA,
                                  String[] headerB, int numToSelect) throws IOException {
    PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(output, false)));
    addHeader(numToSelect, headerA, headerB, writer);

    matches.map(m -> m.getFormattedResults(numToSelect)).forEach(s -> writer.println(s));
  }

  private static void addHeader(int numToSelect, String[] headerA, String[] headerB,
                                PrintWriter writer) {
    StringJoiner header = new StringJoiner("\t");
    for (String h : headerA) {
      header.add(h);
    }
    for (int i = 0; i < numToSelect; i++) {
      header.add("barnacle_" + (i + 1) + "_distance");
      for (int j = 0; j < headerB.length; j++) {
        header.add("barnacle_" + (i + 1) + "_" + headerB[j]);
      }
    }
    header.add("hungarian_selection");
    writer.println(header);
  }

  public static void main(String[] args) {

    // Assumed that the input files are tab delimited with a header, first column is IDs and the
    // next columns are what is to be matched on (e.g tsne1,tsne2).

    Path inputFileAnchor = Paths.get(args[0]);
    // ex. anchors.trim.txt
    // IID tsne1 tsne2
    // 1000173 -43.5954364907359 5.31262265439833
    // 1000891 -59.3878908623605 -74.3238388765456
    Path inputFileBarns = Paths.get(args[1]);
    // ex. barns.trim.txt
    // IID tsne1 tsne2
    // 1000017 -43.5160309060552 -49.3401376767763
    // 1000038 65.4590502813067 -63.8399147505082
    Path ouputDir = Paths.get(args[2]);
    // Number of controls to select initially (maybe 3X the final number needed?). This allows for a
    // buffer of extra controls that can be used for de-duplicating
    int initialNumSelect = Integer.parseInt(args[3]);

    // The actual number of controls that are needed in the end
    int finalNumSelect = Integer.parseInt(args[4]);

    try {
      Instant start = Instant.now();
      run(inputFileAnchor, inputFileBarns, ouputDir, initialNumSelect, finalNumSelect);
      Logger.getAnonymousLogger().info(Duration.between(start, Instant.now()).toString());
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
