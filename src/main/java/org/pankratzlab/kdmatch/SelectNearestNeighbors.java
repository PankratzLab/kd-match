package org.pankratzlab.kdmatch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.logging.Logger;

/**
 * A prototype test for kd-tree based nearest neighbor selection... no real data checks and most
 * things are assumed to be well formed
 *
 */
public class SelectNearestNeighbors {

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

    private String ID;
    // data for this sample (e.g holds PC1-10)
    private double[] dim;
  }

  private static void addToTree(KDTree<Sample> tree, String[] line) {

    Sample s = new Sample(line[0],
                          Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray());
    tree.add(s.dim, s);

  }

  private static ResultHeap<Sample> selectFromTree(KDTree<Sample> tree, String[] line,
                                                   int numToSelect) {

    Sample s = new Sample(line[0],
                          Arrays.stream(line).skip(1).mapToDouble(Double::parseDouble).toArray());
    return tree.getNearestNeighbors(s.dim, numToSelect);

  }

  private static List<String> selectIDMatchesFromTree(KDTree<Sample> tree, String[] line,
                                                      int numToSelect) {
    ResultHeap<Sample> heap = selectFromTree(tree, line, numToSelect);
    List<String> results = new ArrayList<>();
    // The case to be matched
    results.add(line[0]);
    while (heap.size() > 0) {
      results.add(heap.removeMax().ID);
    }

    return results;

  }

  private static void selectAndReportMatchesFromTree(KDTree<Sample> tree, String[] line,
                                                     int numToSelect, PrintWriter writer) {
    List<String> matches = selectIDMatchesFromTree(tree, line, numToSelect);
    writer.println(String.join("\t", matches));

  }

  private static void run(Path inputFileAnchor, Path inputFileBarns, Path ouputDir,
                          int numToSelect) throws IOException {

    Logger log = Logger.getAnonymousLogger();
    String[] headerA = Files.lines(inputFileAnchor).findFirst().toString().trim().split("\t");
    String[] headerB = Files.lines(inputFileBarns).findFirst().toString().trim().split("\t");
    if (Arrays.equals(headerA, headerB)) {

      KDTree<Sample> kd = new KDTree<SelectNearestNeighbors.Sample>(headerA.length - 1);
      log.info("Assuming 1 ID column and " + (headerA.length - 1) + " data columns");

      log.info("building tree from " + inputFileBarns.toString());

      Files.lines(inputFileBarns).map(l -> l.split("\t")).skip(1).forEach(a -> addToTree(kd, a));
      log.info("finished building tree from " + inputFileBarns.toString());

      new File(ouputDir.toString()).mkdirs();
      String output = ouputDir + "test.matchkd.txt";
      PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(output, false)));
      StringJoiner header = new StringJoiner("\t");
      header.add("anchor");
      for (int i = 0; i < numToSelect; i++) {
        header.add("barnacle_ID_" + (i + 1));
      }
      log.info("output file: " + output);
      log.info("selecting and reporting nearest neighbors for  " + inputFileAnchor.toString());
      Files.lines(inputFileAnchor).map(l -> l.split("\t")).skip(1)
           .forEach(a -> selectAndReportMatchesFromTree(kd, a, numToSelect, writer));

      log.info("finished selecting nearest neighbors for  " + inputFileAnchor.toString());
      writer.close();
    } else {
      log.severe("mismatched file headers");
    }

  }

  public static void main(String[] args) {

    // Assumed that the input files are tab delimited with a header, first column is IDs and the
    // remaining is what is to be matched on (e.g tsne1,tsne2).

    Path inputFileAnchor = Paths.get(args[0]);
    Path inputFileBarns = Paths.get(args[1]);
    Path ouputDir = Paths.get(args[2]);
    // Number of controls to select
    int numToSelect = Integer.parseInt(args[3]);

    try {
      run(inputFileAnchor, inputFileBarns, ouputDir, numToSelect);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
