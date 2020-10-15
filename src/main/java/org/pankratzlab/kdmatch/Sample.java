package org.pankratzlab.kdmatch;

import java.util.StringJoiner;

class Sample {
  /**
   * @param iD
   * @param dim
   */
  Sample(String iD, double[] dim) {
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

  String ID;
  // data for this sample (e.g holds PC1-10)
  double[] dim;

  String getOutput() {
    StringJoiner j = new StringJoiner("\t");
    j.add(ID);
    for (int i = 0; i < dim.length; i++) {
      j.add(Double.toString(dim[i]));
    }
    return j.toString();
  }
}
