package org.pankratzlab.kdmatch;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Holds a sample, and the potential matches (i.e nearest neighbors)
 *
 */
public class Match {
  Sample sample;
  List<Sample> matches;
  private Set<String> matchIds;
  private boolean hungarian;

  /**
   * @param sample
   * @param matches
   */
  public Match(Sample sample, List<Sample> matches) {
    super();
    this.sample = sample;
    this.matches = matches;
    this.matchIds = new HashSet<>();
    this.hungarian = false;
  }

  List<Sample> getMatches() {
    return matches;
  }

  /**
   * @param hungarian the hungarian to set
   */
  void setHungarian(boolean hungarian) {
    this.hungarian = hungarian;
  }

  boolean hasMatch(String ID) {

    if (matchIds.size() != matches.size()) {
      matchIds.addAll(getMatchIDList());
    }
    return matchIds.contains(ID);
  }

  /**
   * @return the matchIds
   */
  Set<String> getMatchIdSet() {
    if (matchIds.size() != matches.size()) {
      matchIds.addAll(getMatchIDList());
    }
    return matchIds;
  }

  List<String> getMatchIDList() {
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
      // TODO untested
      if (matches != null && matches.size() > i) {
        Sample control = matches.get(i);
        results.add(control.getID());
        results.add(Double.toString(getDistanceFrom(control)));
        for (int j = 0; j < sample.dim.length; j++) {
          results.add(Double.toString(control.dim[j]));
        }
        if (!control.getGroup().equals("")) {
        	results.add(control.getGroup());
        } else {
        	results.add("no_group");
        }
      } else {
        // TODO untested
    	results.add("no-match");
        results.add(Double.toString(Double.NaN));
        for (int j = 0; j < sample.dim.length; j++) {
          results.add(Double.toString(Double.NaN));
        }
        results.add("NA");
      }
    }
    results.add(Boolean.toString(hungarian));
    return results.toString();

  }
}
