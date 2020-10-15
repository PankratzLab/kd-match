package org.pankratzlab.kdmatch;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.logging.Logger;
import java.util.stream.Collectors;

// Methods:
// 1. select k (k greater than final needed) nearest neighbors for all samples
// 2. prune matches that are completely unique
// 2. Form groups of samples that share a neighbor
// 3. Form cost matrix for community, setting select controls that are not shared to have infinite
// cost for other samples
// 4. Use hungarian algorithm to allocate controls to minimize total distance between cases and
// controls

public class SelectOptimizedNeighbors {
  private static class Sample {
    /**
     * @param iD
     * @param dim
     */
    private Sample(String iD, double[] dim) {
      super();
      ID = iD;
      this.dim = dim;
    }

    /**
     * @return the iD
     */
    String getID() {
      return ID;
    }

    /**
     * @return the dim
     */
    double[] getDim() {
      return dim;
    }

    private String ID;
    // data for this sample (e.g holds PC1-10)
    private double[] dim;
  }

  private static class Match {
    private Sample sample;
    private List<Sample> matches;

    /**
     * @param sample
     * @param matches
     */
    public Match(Sample sample, List<Sample> matches) {
      super();
      this.sample = sample;
      this.matches = matches;
    }

    private List<String> getMatchIDs() {
      return matches.stream().map(s -> s.ID).collect(Collectors.toList());
    }

  }

  private static List<Sample> getMatches(ResultHeap<Sample> matches) {
    List<Sample> results = new ArrayList<>();

    // retrieve sample starting from farthest away (maybe could add a different method in
    // ResultHeap
    while (matches.size() > 0) {
      results.add(matches.removeMax());
    }

    // matches
    // reverse so first index is nearest
    Collections.reverse(results);
    return results;
  }

  private static void addToTree(KDTree<Sample> tree, String[] line) {

    Sample s = new Sample(line[0],
                          Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray());
    tree.add(s.dim, s);

  }

  private static Match getMatchFromTree(KDTree<Sample> tree, String[] line, int numToSelect) {

    Sample s = new Sample(line[0],
                          Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray());
    return new Match(s, getMatches(tree.getNearestNeighbors(s.dim, numToSelect)));

  }

  private static ResultHeap<Sample> selectFromTree(KDTree<Sample> tree, String[] line,
                                                   int numToSelect) {

    Sample s = new Sample(line[0],
                          Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray());
    return tree.getNearestNeighbors(s.dim, numToSelect);

  }

  /**
   * 
   */
  private static double getEuclidDistance(double[] p1, double[] p2) {
    double sum = 0;
    for (int i = 0; i < p1.length; i++) {
      final double dp = p1[i] - p2[i];
      sum += dp * dp;
    }
    return Math.sqrt(sum);
  }

  private static List<Sample> getSelectionsForLine(KDTree<Sample> tree, String[] line,
                                                   int numToSelect) {
    ResultHeap<Sample> heap = selectFromTree(tree, line, numToSelect);
    List<Sample> results = new ArrayList<>();

    // retrieve sample starting from farthest away (maybe could add a different method in
    // ResultHeap
    while (heap.size() > 0) {
      results.add(heap.removeMax());
    }
    // reverse so first index is nearest
    Collections.reverse(results);
    return results;
  }

  private static void run(Path inputFileAnchor, Path inputFileBarns, Path ouputDir,
                          int numToSelect) throws IOException {

    Logger log = Logger.getAnonymousLogger();
    String[] headerA = Files.lines(inputFileAnchor).findFirst().get().toString().trim().split("\t");
    String[] headerB = Files.lines(inputFileBarns).findFirst().get().toString().trim().split("\t");
    if (Arrays.equals(headerA, headerB)) {

      // Could instead use a Map (factor[i.e phenograph_cluster] -> tree) to build individual trees
      // split within each cluster.
      // Similar for when querying
      KDTree<Sample> kd = new KDTree<Sample>(headerA.length - 1);
      log.info("Assuming 1 ID column and " + (headerA.length - 1) + " data columns");

      log.info("building tree from " + inputFileBarns.toString());
      Files.lines(inputFileBarns).map(l -> l.split("\t")).skip(1).forEach(a -> addToTree(kd, a));
      log.info("finished building tree from " + inputFileBarns.toString());

      new File(ouputDir.toString()).mkdirs();
      String output = ouputDir + "test.match.NoDups.txt";
      // PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(output, false)));
      //
      // addHeader(numToSelect, headerA, headerB, writer);
      // writer.close();

      log.info("output file: " + output);
      log.info("selecting nearest neighbors for  " + inputFileAnchor.toString());

      List<Match> matches = Files.lines(inputFileAnchor).map(l -> l.split("\t")).skip(1)
                                 .map(a -> getMatchFromTree(kd, a, numToSelect))
                                 .collect(Collectors.toList());

      log.info("finished selecting nearest neighbors for  " + matches.size() + " anchors in "
               + inputFileAnchor.toString());

      log.info("counting occurrences of each control" + inputFileAnchor.toString());

      Map<String, Long> counts = matches.stream().map(m -> m.matches).flatMap(List::stream)
                                        .collect(Collectors.groupingBy(m -> m.getID(),
                                                                       Collectors.counting()));

      log.info("finding duplicated controls");

      Map<String, Long> duplicatedControls = counts.entrySet().stream()
                                                   .filter(map -> map.getValue() > 1)
                                                   .collect(Collectors.toMap(map -> map.getKey(),
                                                                             map -> map.getValue()));
      log.info("found " + duplicatedControls.size() + " duplicated controls");

      log.info("pruning selections that are uniquely matched at baseline");

      List<Match> baselineUniqueMatches = matches.stream()
                                                 .filter(m -> m.getMatchIDs().stream()
                                                               .noneMatch(s -> duplicatedControls.containsKey(s)))
                                                 .collect(Collectors.toList());

      List<Match> baselineDuplicateMatches = matches.stream()
                                                    .filter(m -> m.getMatchIDs().stream()
                                                                  .anyMatch(s -> duplicatedControls.containsKey(s)))
                                                    .collect(Collectors.toList());
      log.info(baselineUniqueMatches.size() + " selections are uniquely matched at baseline");
      log.info(baselineDuplicateMatches.size() + " selections are uniquely matched at baseline");

    } else {
      log.severe("mismatched file headers");
    }

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
    // Number of controls to select
    int numToSelect = Integer.parseInt(args[3]);

    // int finalNumNeeded = Integer.parseInt(args[3]);

    try {
      run(inputFileAnchor, inputFileBarns, ouputDir, numToSelect);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
