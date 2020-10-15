package org.pankratzlab.kdmatch;

public class Utils {
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
}
