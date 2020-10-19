package org.pankratzlab.kdmatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SelectOptimizedNeighbors {

  private static void addToTree(KDTree<Sample> tree, Sample sample) {
    tree.add(sample.dim, sample);

  }

  static void addSamplesToTree(KDTree<Sample> tree, Stream<Sample> barnacles) {
    barnacles.forEach(s -> addToTree(tree, s));
  }

  private static List<Sample> getMatches(ResultHeap<Sample> matches) {
    List<Sample> results = new ArrayList<>();

    // retrieve sample, starting from farthest away
    while (matches.size() > 0) {
      results.add(matches.removeMax());
    }

    // reverse so first index is the nearest distance match
    Collections.reverse(results);
    return results;
  }

  static Stream<Match> getNearestNeighborsForSamples(KDTree<Sample> tree, Stream<Sample> anchors,
                                                     int numToSelect) {
    return anchors.map(a -> new Match(a, getMatches(tree.getNearestNeighbors(a.dim, numToSelect))));

  }

  private static boolean connected(Set<String> idSet, Match match) {
    return !Collections.disjoint(idSet, match.getMatchIdSet());
  }

  // TODO this will not scale super well
  private static List<List<Match>> getCommunities(List<Match> matchesWithDuplicates) {

    CommunityDetectionGraph g = new CommunityDetectionGraph(matchesWithDuplicates.size());
    for (int i = 0; i < matchesWithDuplicates.size(); i++) {
      Set<String> idi = matchesWithDuplicates.get(i).getMatchIdSet();
      for (int j = 0; j < matchesWithDuplicates.size(); j++) {
        // do not need to compare to self, and is an undirected graph ... so i > j
        if (i > j) {
          if (connected(idi, matchesWithDuplicates.get(j))) {
            g.addEdge(i, j);
          }
        }
      }
    }

    List<List<Match>> communities = new ArrayList<List<Match>>();
    List<List<Integer>> connections = g.connectedComponents();
    for (List<Integer> community : connections) {
      List<Match> current = new ArrayList<>();
      for (Integer m : community) {
        current.add(matchesWithDuplicates.get(m));
      }
      communities.add(current);
    }
    return communities;
  }

  static Stream<Match> optimizeDuplicates(List<Match> matches, int numSelect, int threads,
                                          Logger log) throws InterruptedException,
                                                      ExecutionException {

    log.info("counting occurrences of each control and finding duplicates");

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

    // These matches do not share any controls with another case, so are done
    List<Match> uniqueMatches = matches.stream()
                                       .filter(m -> m.getMatchIDList().stream()
                                                     .noneMatch(s -> duplicatedControlCounts.containsKey(s)))
                                       .collect(Collectors.toList());
    log.info(uniqueMatches.size() + " selections are uniquely matched at baseline");

    // These matches share at least one control with another case, so will be optimized
    List<Match> matchesWithDuplicates = matches.stream()
                                               .filter(m -> m.getMatchIDList().stream()
                                                             .anyMatch(s -> duplicatedControlCounts.containsKey(s)))
                                               .collect(Collectors.toList());
    log.info(matchesWithDuplicates.size() + " selections have non-unique matches at baseline");

    if (matchesWithDuplicates.size() > 0) {
      // form connected communities
      log.info("Forming  communities");

      List<List<Match>> communities = getCommunities(matchesWithDuplicates);
      log.info("Optimizing selection within " + communities.size() + " communities");

      // Holds matches post optimization
      List<Match> optimizedMatches = new ForkJoinPool(threads).submit(() ->

      // parallel stream invoked here to process each community individually (not sure if it is
      // actually
      // working, but could be parallelized another way)
      communities.parallelStream().map(e -> getOptimizedMatches(e, numSelect, log))
                 .flatMap(List::stream).collect(Collectors.toList())

      ).get();

      if (optimizedMatches.size() != matchesWithDuplicates.size()) {
        throw new IllegalStateException("Mismatched number of matches");

      }
      uniqueMatches.addAll(optimizedMatches);
    }
    return uniqueMatches.stream();

  }

  private static List<Match> getOptimizedMatches(final List<Match> matchesWithDuplicates,
                                                 int numSelect, Logger log) {
    // Extract all unique control Samples that are matched to at least two cases
    List<Sample> allUniqueControls = matchesWithDuplicates.stream().map(m -> m.getMatches())
                                                          .flatMap(mlist -> mlist.stream())
                                                          .filter(Utils.distinctByKey(c -> c.getID()))
                                                          .collect(Collectors.toList());

    Map<Integer, Integer> mapOptimize = new HashMap<Integer, Integer>();

    double[][] costMatrix = new double[matchesWithDuplicates.size()
                                       * numSelect][allUniqueControls.size()];

    int row = 0;
    // We replicate the cases in multiple rows so that "numSelect" of each case are evaluated at the
    // same time
    for (int i = 0; i < numSelect; i++) {

      int map = 0;
      for (Match match : matchesWithDuplicates) {
        mapOptimize.put(row, map);
        map++;
        int col = 0;
        for (Sample sample : allUniqueControls) {
          if (match.hasMatch(sample.ID)) {
            costMatrix[row][col] = match.getDistanceFrom(sample);
          } else {
            // set to max value if the sample does not share a particular control as a nearest
            // neighbor
            costMatrix[row][col] = Double.MAX_VALUE;
          }
          col++;
        }
        row++;
      }
    }

    int[] selections = new HungarianAlgorithm(costMatrix).execute();
    // initialize new optimized matches
    List<Match> optimizedMatches = new ArrayList<>(matchesWithDuplicates.size());
    matchesWithDuplicates.stream().map(d -> new Match(d.sample, new ArrayList<>()))
                         .forEachOrdered(optimizedMatches::add);

    for (int j = 0; j < selections.length; j++) {
      // -1 means the case could not be matched
      if (selections[j] >= 0) {
        Sample selection = allUniqueControls.get(selections[j]);
        optimizedMatches.get(mapOptimize.get(j)).matches.add(selection);
        int size = optimizedMatches.get(mapOptimize.get(j)).matches.size();

        // Check if the order of controls has been updated for this matching
        if (!matchesWithDuplicates.get(mapOptimize.get(j)).matches.get(size - 1).getID()
                                                                  .equals(selection.getID())) {
          optimizedMatches.get(mapOptimize.get(j)).setHungarian(true);

        }
      }
    }
    return optimizedMatches;
  }
}
