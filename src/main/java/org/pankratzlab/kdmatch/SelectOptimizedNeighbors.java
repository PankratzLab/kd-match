package org.pankratzlab.kdmatch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
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

    private String ID;
    // data for this sample (e.g holds PC1-10)
    private double[] dim;

  }

  private static class Match {
    private Sample sample;
    private List<Sample> matches;
    private Set<String> matchIds;

    /**
     * @param sample
     * @param matches
     */
    public Match(Sample sample, List<Sample> matches) {
      super();
      this.sample = sample;
      this.matches = matches;
      this.matchIds = new HashSet<>();
    }

    private List<Sample> getMatches() {
      return matches;
    }

    private boolean hasMatch(String ID) {

      if (matchIds.size() != matches.size()) {
        matchIds.addAll(getMatchIDs());
      }
      return matchIds.contains(ID);
    }

    private List<String> getMatchIDs() {
      return matches.stream().map(s -> s.ID).collect(Collectors.toList());
    }

    private double getDistanceFrom(Sample other) {
      return getEuclidDistance(sample.dim, other.dim);
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

  // https://www.baeldung.com/java-streams-distinct-by
  private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {

    Map<Object, Boolean> seen = new ConcurrentHashMap<>();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
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

  private static void run(Path inputFileAnchor, Path inputFileBarns, Path ouputDir,
                          int initialNumSelect, int finalNumSelect) throws IOException {

    Logger log = Logger.getAnonymousLogger();
    String[] headerA = Files.lines(inputFileAnchor).findFirst().get().toString().trim().split("\t");
    String[] headerB = Files.lines(inputFileBarns).findFirst().get().toString().trim().split("\t");
    if (Arrays.equals(headerA, headerB)) {

      KDTree<Sample> kd = new KDTree<Sample>(headerA.length - 1);// dimension of the data to
                                                                 // searched
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
                                 .map(a -> getMatchFromTree(kd, a, initialNumSelect))
                                 .collect(Collectors.toList());

      log.info("finished selecting nearest neighbors for  " + matches.size() + " anchors in "
               + inputFileAnchor.toString());

      log.info("counting occurrences of each control and finding duplicated controls");

      Map<String, Long> duplicatedControlCounts = matches.stream().map(m -> m.matches)
                                                         .flatMap(List::stream)
                                                         .collect(Collectors.groupingBy(m -> m.getID(),
                                                                                        Collectors.counting()))
                                                         .entrySet().stream()
                                                         .filter(map -> map.getValue() > 1)
                                                         .collect(Collectors.toMap(map -> map.getKey(),
                                                                                   map -> map.getValue()));
      log.info("found " + duplicatedControlCounts.size() + " duplicated controls");

      log.info("pruning selections that are uniquely matched at baseline");

      List<Match> baselineUniqueMatches = matches.stream()
                                                 .filter(m -> m.getMatchIDs().stream()
                                                               .noneMatch(s -> duplicatedControlCounts.containsKey(s)))
                                                 .collect(Collectors.toList());

      List<Match> baselineDuplicateMatches = matches.stream()
                                                    .filter(m -> m.getMatchIDs().stream()
                                                                  .anyMatch(s -> duplicatedControlCounts.containsKey(s)))
                                                    .collect(Collectors.toList());
      log.info(baselineUniqueMatches.size() + " selections are uniquely matched at baseline");
      log.info(baselineDuplicateMatches.size() + " selections are duplicated at baseline");

      List<Sample> allDuplicatedcontrols = baselineDuplicateMatches.stream()
                                                                   .map(m -> m.getMatches())
                                                                   .flatMap(mlist -> mlist.stream())
                                                                   .filter(distinctByKey(c -> c.getID()))
                                                                   .collect(Collectors.toList());

      log.info(allDuplicatedcontrols.size() + " total controls to optimize");

      List<Match> optimizedMatches = new ArrayList<>(baselineDuplicateMatches.size());
      baselineDuplicateMatches.stream().map(d -> new Match(d.sample, new ArrayList<>()))
                              .forEachOrdered(optimizedMatches::add);

      for (int i = 0; i < finalNumSelect; i++) {

        double[][] costMatrix = new double[baselineDuplicateMatches.size()][allDuplicatedcontrols.size()];

        int row = 0;
        for (Match match : baselineDuplicateMatches) {
          int col = 0;
          for (Sample sample : allDuplicatedcontrols) {
            if (match.hasMatch(sample.ID)) {
              costMatrix[row][col] = match.getDistanceFrom(sample);
            } else {
              costMatrix[row][col] = Double.MAX_VALUE;
            }
            col++;
          }
          row++;
        }
        log.info("Selecting optimal set from duplicated controls");
        HungarianAlgorithm hg = new HungarianAlgorithm(costMatrix);
        int[] selections = hg.execute();
        log.info("Selecting round number " + i + " for total matches:" + selections.length);

        Set<String> toRemove = new HashSet<>();
        for (int j = 0; j < selections.length; j++) {
          optimizedMatches.get(j).matches.add(allDuplicatedcontrols.get(selections[j]));
          toRemove.add(allDuplicatedcontrols.get(selections[j]).ID);
        }
        allDuplicatedcontrols = allDuplicatedcontrols.stream().filter(c -> !toRemove.contains(c.ID))
                                                     .collect(Collectors.toList());
        log.info("New number of controls to select from " + allDuplicatedcontrols.size());
      }

      // optimizedMatches

      Map<String, Long> countsOpt = optimizedMatches.stream().map(m -> m.matches)
                                                    .flatMap(List::stream)
                                                    .collect(Collectors.groupingBy(m -> m.getID(),
                                                                                   Collectors.counting()));

      log.info("finding duplicated controls");

      Map<String, Long> duplicatedControlCountOpts = countsOpt.entrySet().stream()
                                                              .filter(map -> map.getValue() > 1)
                                                              .collect(Collectors.toMap(map -> map.getKey(),
                                                                                        map -> map.getValue()));
      log.info(duplicatedControlCountOpts.size() + " duplicates remain");

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
    int initialNumSelect = Integer.parseInt(args[3]);

    int finalNumSelect = Integer.parseInt(args[4]);
    // int finalNumNeeded = Integer.parseInt(args[3]);

    try {
      run(inputFileAnchor, inputFileBarns, ouputDir, initialNumSelect, finalNumSelect);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
