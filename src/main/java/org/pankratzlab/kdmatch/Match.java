package org.pankratzlab.kdmatch;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class Match {
  Sample sample;
  List<Sample> matches;
  private Set<String> matchIds;
  private boolean hungarian;

  /**
   * @param sample
   * @param matches
   */
  public Match(Sample sample, List<Sample> matches, boolean hungarian) {
    super();
    this.sample = sample;
    this.matches = matches;
    this.matchIds = new HashSet<>();
    this.hungarian = hungarian;
  }

  List<Sample> getMatches() {
    return matches;
  }

  boolean hasMatch(String ID) {

    if (matchIds.size() != matches.size()) {
      matchIds.addAll(getMatchIDs());
    }
    return matchIds.contains(ID);
  }

  List<String> getMatchIDs() {
    return matches.stream().map(s -> s.ID).collect(Collectors.toList());
  }

  double getDistanceFrom(Sample other) {
    return Utils.getEuclidDistance(sample.dim, other.dim);
  }

  String getFormattedResults(int numToSelect) {
    StringJoiner results = new StringJoiner("\t");
    // The case to be matched
    results.add(sample.getOutput());

    for (int i = 0; i < numToSelect; i++) {
      Sample control = matches.get(i);
      results.add(Double.toString(getDistanceFrom(control)));
      results.add(control.ID);
      for (int j = 0; j < sample.dim.length; j++) {
        results.add(Double.toString(control.dim[j]));
      }

    }
    results.add(Boolean.toString(hungarian));

    return results.toString();

  }
}
