package org.pankratzlab.kdmatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // reverse so first index is nearest distance match
    Collections.reverse(results);
    return results;
  }

  static Stream<Match> getNearestNeighborsForSamples(KDTree<Sample> tree, Stream<Sample> anchors,
                                                     int numToSelect) {
    return anchors.map(a -> new Match(a, getMatches(tree.getNearestNeighbors(a.dim, numToSelect))));

  }

  static Stream<Match> optimizeDuplicates(List<Match> matches, int numSelect, Logger log) {

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
                                       .filter(m -> m.getMatchIDs().stream()
                                                     .noneMatch(s -> duplicatedControlCounts.containsKey(s)))
                                       .collect(Collectors.toList());
    log.info(uniqueMatches.size() + " selections are uniquely matched at baseline");

    // These matches share at least one control with another case, so will be optimized
    List<Match> matchesWithDuplicates = matches.stream()
                                               .filter(m -> m.getMatchIDs().stream()
                                                             .anyMatch(s -> duplicatedControlCounts.containsKey(s)))
                                               .collect(Collectors.toList());
    log.info(matchesWithDuplicates.size() + " selections have non-unique matches at baseline");

    if (matchesWithDuplicates.size() > 0) {
      // TODO perform within a community

      // Get all control Samples that are matched to at least two cases
      List<Sample> allDuplicatedcontrols = matchesWithDuplicates.stream().map(m -> m.getMatches())
                                                                .flatMap(mlist -> mlist.stream())
                                                                .filter(Utils.distinctByKey(c -> c.getID()))
                                                                .collect(Collectors.toList());

      log.info(allDuplicatedcontrols.size() + " total controls to de-duplicate");
      // Holds matches post optimization
      List<Match> optimizedMatches = getOptimizedMatches(matchesWithDuplicates,
                                                         allDuplicatedcontrols, numSelect, log);

      uniqueMatches.addAll(optimizedMatches);
    }
    return uniqueMatches.stream();

  }

  static List<Match> getOptimizedMatches(final List<Match> baselineMatchesWithDuplicates,
                                         final List<Sample> allDuplicatedcontrols, int numSelect,
                                         Logger log) {

    Map<Integer, Integer> mapOptimize = new HashMap<Integer, Integer>();

    log.info("Selecting optimal matches and removing duplicates");

    double[][] costMatrix = new double[baselineMatchesWithDuplicates.size()
                                       * numSelect][allDuplicatedcontrols.size()];

    int row = 0;
    // We replicate the cases so that "numSelect" controls are evaluated concurrently
    for (int i = 0; i < numSelect; i++) {

      int map = 0;
      for (Match match : baselineMatchesWithDuplicates) {
        mapOptimize.put(row, map);
        map++;
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
    }

    int[] selections = new HungarianAlgorithm(costMatrix).execute();
    log.info(selections.length + "");
    List<Match> optimizedMatches = new ArrayList<>(baselineMatchesWithDuplicates.size());
    baselineMatchesWithDuplicates.stream().map(d -> new Match(d.sample, new ArrayList<>()))
                                 .forEachOrdered(optimizedMatches::add);

    for (int j = 0; j < selections.length; j++) {
      // -1 means the case could not be matched
      if (selections[j] >= 0) {
        Sample selection = allDuplicatedcontrols.get(selections[j]);
        optimizedMatches.get(mapOptimize.get(j)).matches.add(selection);
        int size = optimizedMatches.get(mapOptimize.get(j)).matches.size();

        // Check if the order of controls has been updated for this matching
        if (!baselineMatchesWithDuplicates.get(mapOptimize.get(j)).matches.get(size - 1).getID()
                                                                          .equals(selection.getID())) {
          optimizedMatches.get(mapOptimize.get(j)).setHungarian(true);

        }
      }
    }
    return optimizedMatches;
  }

}
