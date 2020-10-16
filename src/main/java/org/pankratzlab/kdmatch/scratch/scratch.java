package org.pankratzlab.kdmatch.scratch;

import java.util.List;

public class scratch {
  // private static int[] getOptimizedMatch(List<Match> baselineMatchesWithDuplicates,
  // List<Sample> allDuplicatedcontrols) {
  // double[][] costMatrix = new
  // double[baselineMatchesWithDuplicates.size()][allDuplicatedcontrols.size()];
  //
  // int row = 0;
  // for (Match match : baselineMatchesWithDuplicates) {
  // int col = 0;
  // for (Sample sample : allDuplicatedcontrols) {
  // if (match.hasMatch(sample.ID)) {
  // costMatrix[row][col] = match.getDistanceFrom(sample);
  // } else {
  // costMatrix[row][col] = Double.MAX_VALUE;
  // }
  // col++;
  // }
  // row++;
  // }
  // return new HungarianAlgorithm(costMatrix).execute();
  // }
  // package org.pankratzlab.kdmatch;
  //
  // import java.io.BufferedWriter;
  // import java.io.File;
  // import java.io.FileWriter;
  // import java.io.IOException;
  // import java.io.PrintWriter;
  // import java.nio.file.Files;
  // import java.nio.file.Path;
  // import java.nio.file.Paths;
  // import java.util.ArrayList;
  // import java.util.Arrays;
  // import java.util.Collections;
  // import java.util.List;
  // import java.util.StringJoiner;
  // import java.util.logging.Logger;
  //
  // /**
  // * A prototype test for kd-tree based nearest neighbor selection... no real data checks and most
  // * things are assumed to be well formed
  // *
  // */
  // public class SelectNearestNeighbors {
  //
  // private static class Sample {
  // /**
  // * @param iD
  // * @param dim
  // */
  // private Sample(String iD, double[] dim) {
  // super();
  // ID = iD;
  // this.dim = dim;
  // }
  //
  // private String ID;
  // // data for this sample (e.g holds PC1-10)
  // private double[] dim;
  // }
  //
  // private static void addToTree(KDTree<Sample> tree, String[] line) {
  //
  // Sample s = new Sample(line[0],
  // Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray());
  // tree.add(s.dim, s);
  //
  // }
  //
  // private static ResultHeap<Sample> selectFromTree(KDTree<Sample> tree, String[] line,
  // int numToSelect) {
  //
  // Sample s = new Sample(line[0],
  // Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray());
  // return tree.getNearestNeighbors(s.dim, numToSelect);
  //
  // }
  //
  // /**
  // *
  // */
  // private static double getEuclidDistance(double[] p1, double[] p2) {
  // double sum = 0;
  // for (int i = 0; i < p1.length; i++) {
  // final double dp = p1[i] - p2[i];
  // sum += dp * dp;
  // }
  // return Math.sqrt(sum);
  // }
  //
  // private static List<Sample> getSelectionsForLine(KDTree<Sample> tree, String[] line,
  // int numToSelect) {
  // ResultHeap<Sample> heap = selectFromTree(tree, line, numToSelect);
  // List<Sample> results = new ArrayList<>();
  //
  // // retrieve sample starting from farthest away (maybe could add a different method in
  // ResultHeap
  // while (heap.size() > 0) {
  // results.add(heap.removeMax());
  // }
  // // reverse so first index is nearest
  // Collections.reverse(results);
  // return results;
  // }
  //
  // private static List<String> getFormattedResults(KDTree<Sample> tree, String[] line,
  // int numToSelect) {
  // List<String> results = new ArrayList<>();
  // // The case to be matched
  // results.addAll(Arrays.asList(line));
  // double[] caseLoc = Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray();
  // List<Sample> samples = getSelectionsForLine(tree, line, numToSelect);
  // for (Sample control : samples) {
  // results.add(Double.toString(getEuclidDistance(caseLoc, control.dim)));
  // results.add(control.ID);
  // for (int i = 0; i < control.dim.length; i++) {
  // results.add(Double.toString(control.dim[i]));
  //
  // }
  // }
  //
  // return results;
  //
  // }
  //
  // private static void selectAndReportMatchesFromTree(KDTree<Sample> tree, String[] line,
  // int numToSelect, PrintWriter writer) {
  // List<String> matches = getFormattedResults(tree, line, numToSelect);
  // writer.println(String.join("\t", matches));
  //
  // }
  //
  // private static void addHeader(int numToSelect, String[] headerA, String[] headerB,
  // PrintWriter writer) {
  // StringJoiner header = new StringJoiner("\t");
  // for (String h : headerA) {
  // header.add(h);
  // }
  // for (int i = 0; i < numToSelect; i++) {
  // header.add("barnacle_" + (i + 1) + "_distance");
  // for (int j = 0; j < headerB.length; j++) {
  // header.add("barnacle_" + (i + 1) + "_" + headerB[j]);
  // }
  // }
  // writer.println(header);
  // }
  //
  // private static void run(Path inputFileAnchor, Path inputFileBarns, Path ouputDir,
  // int numToSelect) throws IOException {
  //
  // Logger log = Logger.getAnonymousLogger();
  // String[] headerA =
  // Files.lines(inputFileAnchor).findFirst().get().toString().trim().split("\t");
  // String[] headerB = Files.lines(inputFileBarns).findFirst().get().toString().trim().split("\t");
  // if (Arrays.equals(headerA, headerB)) {
  //
  // // Could instead use a Map (factor[i.e phenograph_cluster] -> tree) to build individual trees
  // // split within each cluster.
  // // Similar for when querying
  // KDTree<Sample> kd = new KDTree<SelectNearestNeighbors.Sample>(headerA.length - 1);
  // log.info("Assuming 1 ID column and " + (headerA.length - 1) + " data columns");
  //
  // log.info("building tree from " + inputFileBarns.toString());
  //
  // Files.lines(inputFileBarns).map(l -> l.split("\t")).skip(1).forEach(a -> addToTree(kd, a));
  // log.info("finished building tree from " + inputFileBarns.toString());
  //
  // new File(ouputDir.toString()).mkdirs();
  // String output = ouputDir + "test.match.allowDups.txt";
  // PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(output, false)));
  // addHeader(numToSelect, headerA, headerB, writer);
  // log.info("output file: " + output);
  // log.info("selecting and reporting nearest neighbors for " + inputFileAnchor.toString());
  // Files.lines(inputFileAnchor).map(l -> l.split("\t")).skip(1)
  // .forEach(a -> selectAndReportMatchesFromTree(kd, a, numToSelect, writer));
  //
  // log.info("finished selecting nearest neighbors for " + inputFileAnchor.toString());
  // writer.close();
  // } else {
  // log.severe("mismatched file headers");
  // }
  //
  // }
  //
  // public static void main(String[] args) {
  //
  // // Assumed that the input files are tab delimited with a header, first column is IDs and the
  // // next columns are what is to be matched on (e.g tsne1,tsne2).
  //
  // Path inputFileAnchor = Paths.get(args[0]);
  // Path inputFileBarns = Paths.get(args[1]);
  // Path ouputDir = Paths.get(args[2]);
  // // Number of controls to select
  // int numToSelect = Integer.parseInt(args[3]);
  //
  // try {
  // run(inputFileAnchor, inputFileBarns, ouputDir, numToSelect);
  // } catch (IOException e) {
  // e.printStackTrace();
  // }
  //
  // }
  // }

  // static Stream<Match> optimizeDuplicates(List<Match> matches, int numSelect, Logger log) {
  //
  // log.info("counting occurrences of each control and finding duplicates");
  //
  // Map<String, Long> duplicatedControlCounts = matches.stream().map(m -> m.matches)
  // .flatMap(List::stream)
  // .collect(Collectors.groupingBy(m -> m.getID(),
  // Collectors.counting()))
  // .entrySet().stream()
  // .filter(map -> map.getValue() > 1)
  // .collect(Collectors.toMap(map -> map.getKey(),
  // map -> map.getValue()));
  // log.info("found " + duplicatedControlCounts.size() + " duplicated controls");
  //
  // log.info("pruning selections that are uniquely matched at baseline");
  //
  // List<Match> baselineUniqueMatches = matches.stream()
  // .filter(m -> m.getMatchIDs().stream()
  // .noneMatch(s -> duplicatedControlCounts.containsKey(s)))
  // .collect(Collectors.toList());
  //
  // List<Match> baselineMatchesWithDuplicates = matches.stream()
  // .filter(m -> m.getMatchIDs().stream()
  // .anyMatch(s -> duplicatedControlCounts.containsKey(s)))
  // .collect(Collectors.toList());
  // log.info(baselineUniqueMatches.size() + " selections are uniquely matched at baseline");
  // log.info(baselineMatchesWithDuplicates.size() + " selections are duplicated at baseline");
  //
  // // Get all control Samples that are matched to at least two cases
  // List<Sample> allDuplicatedcontrols = baselineMatchesWithDuplicates.stream()
  // .map(m -> m.getMatches())
  // .flatMap(mlist -> mlist.stream())
  // .filter(Utils.distinctByKey(c -> c.getID()))
  // .collect(Collectors.toList());
  //
  // log.info(allDuplicatedcontrols.size() + " total controls to de-duplicate");
  //
  // if (allDuplicatedcontrols.size() > 0) {
  // // Holds matches post optimization
  // List<Match> optimizedMatches = new ArrayList<>(baselineMatchesWithDuplicates.size());
  // baselineMatchesWithDuplicates.stream().map(d -> new Match(d.sample, new ArrayList<>()))
  // .forEachOrdered(optimizedMatches::add);
  //
  // log.info("Selecting optimal matches and removing duplicates");
  //
  // for (int i = 0; i < numSelect; i++) {
  // log.info("Selecting round number " + i + " for total matches:"
  // + baselineMatchesWithDuplicates.size());
  //
  // int[] selections = getOptimizedMatch(baselineMatchesWithDuplicates, allDuplicatedcontrols);
  //
  // Set<String> toRemove = new HashSet<>();
  // for (int j = 0; j < selections.length; j++) {
  // // -1 means the case could not be matched
  // if (selections[j] >= 0) {
  // Sample selection = allDuplicatedcontrols.get(selections[j]);
  // optimizedMatches.get(j).matches.add(selection);
  //
  // // Note if the order of controls has changed for this matching
  // if (!baselineMatchesWithDuplicates.get(j).matches.get(i).getID()
  // .equals(selection.getID())) {
  // optimizedMatches.get(j).setHungarian(true);
  //
  // }
  // toRemove.add(allDuplicatedcontrols.get(selections[j]).ID);
  // }
  // }
  // // Remove controls that have been selected in this round
  // allDuplicatedcontrols = allDuplicatedcontrols.stream().filter(c -> !toRemove.contains(c.ID))
  // .collect(Collectors.toList());
  // log.info("New number of controls to select from:" + allDuplicatedcontrols.size());
  // }
  //
  // baselineUniqueMatches.addAll(optimizedMatches);
  // }
  // return baselineUniqueMatches.stream();
  //
  // }
  //
  // private static int[] getOptimizedMatch(List<Match> baselineMatchesWithDuplicates,
  // List<Sample> allDuplicatedcontrols) {
  // double[][] costMatrix = new
  // double[baselineMatchesWithDuplicates.size()][allDuplicatedcontrols.size()];
  //
  // int row = 0;
  // for (Match match : baselineMatchesWithDuplicates) {
  // int col = 0;
  // for (Sample sample : allDuplicatedcontrols) {
  // if (match.hasMatch(sample.ID)) {
  // costMatrix[row][col] = match.getDistanceFrom(sample);
  // } else {
  // costMatrix[row][col] = Double.MAX_VALUE;
  // }
  // col++;
  // }
  // row++;
  // }
  // return new HungarianAlgorithm(costMatrix).execute();
  // }
}
