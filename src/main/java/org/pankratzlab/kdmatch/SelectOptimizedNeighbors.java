package org.pankratzlab.kdmatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    // retrieve sample starting from farthest away (maybe could add a different method in
    // ResultHeap
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

    List<Match> baselineUniqueMatches = matches.stream()
                                               .filter(m -> m.getMatchIDs().stream()
                                                             .noneMatch(s -> duplicatedControlCounts.containsKey(s)))
                                               .collect(Collectors.toList());

    List<Match> baselineMatchesWithDuplicates = matches.stream()
                                                       .filter(m -> m.getMatchIDs().stream()
                                                                     .anyMatch(s -> duplicatedControlCounts.containsKey(s)))
                                                       .collect(Collectors.toList());
    log.info(baselineUniqueMatches.size() + " selections are uniquely matched at baseline");
    log.info(baselineMatchesWithDuplicates.size() + " selections are duplicated at baseline");

    // Get all control Samples that are matched to at least two cases
    List<Sample> allDuplicatedcontrols = baselineMatchesWithDuplicates.stream()
                                                                      .map(m -> m.getMatches())
                                                                      .flatMap(mlist -> mlist.stream())
                                                                      .filter(Utils.distinctByKey(c -> c.getID()))
                                                                      .collect(Collectors.toList());

    log.info(allDuplicatedcontrols.size() + " total controls to de-duplicate");

    if (allDuplicatedcontrols.size() > 0) {
      // Holds matches post optimization
      List<Match> optimizedMatches = new ArrayList<>(baselineMatchesWithDuplicates.size());
      baselineMatchesWithDuplicates.stream().map(d -> new Match(d.sample, new ArrayList<>()))
                                   .forEachOrdered(optimizedMatches::add);

      log.info("Selecting optimal matches and removing duplicates");

      for (int i = 0; i < numSelect; i++) {
        log.info("Selecting round number " + i + " for total matches:"
                 + baselineMatchesWithDuplicates.size());

        int[] selections = getOptimizedMatch(baselineMatchesWithDuplicates, allDuplicatedcontrols);

        Set<String> toRemove = new HashSet<>();
        for (int j = 0; j < selections.length; j++) {
          optimizedMatches.get(j).matches.add(allDuplicatedcontrols.get(selections[j]));

          // Note if the order of controls has changed
          if (!baselineMatchesWithDuplicates.get(j).matches.get(i).getID()
                                                           .equals(allDuplicatedcontrols.get(selections[j])
                                                                                        .getID())) {
            optimizedMatches.get(j).setHungarian(true);

          }
          toRemove.add(allDuplicatedcontrols.get(selections[j]).ID);
        }
        // Remove controls that have been selected in this round
        allDuplicatedcontrols = allDuplicatedcontrols.stream().filter(c -> !toRemove.contains(c.ID))
                                                     .collect(Collectors.toList());
        log.info("New number of controls to select from:" + allDuplicatedcontrols.size());
      }

      baselineUniqueMatches.addAll(optimizedMatches);
    }
    return baselineUniqueMatches.stream();

  }

  private static int[] getOptimizedMatch(List<Match> baselineMatchesWithDuplicates,
                                         List<Sample> allDuplicatedcontrols) {
    double[][] costMatrix = new double[baselineMatchesWithDuplicates.size()][allDuplicatedcontrols.size()];

    int row = 0;
    for (Match match : baselineMatchesWithDuplicates) {
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
    return new HungarianAlgorithm(costMatrix).execute();
  }

}
