package org.pankratzlab.kdmatch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

class Utils {
  private Utils() {}

  /**
   * 
   */
  static double getEuclidDistance(double[] p1, double[] p2) {
    double sum = 0;
    for (int i = 0; i < p1.length; i++) {
      final double dp = p1[i] - p2[i];
      sum += dp * dp;
    }
    return Math.sqrt(sum);
  }

  // https://www.baeldung.com/java-streams-distinct-by
  static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {

    Map<Object, Boolean> seen = new ConcurrentHashMap<>();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }
}
