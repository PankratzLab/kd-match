package org.pankratzlab.kdmatch;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// Modified from https://www.geeksforgeeks.org/connected-components-in-an-undirected-graph/
public class CommunityDetectionGraph {
  // A user define class to represent a graph.
  // A graph is an array of adjacency lists.
  // Size of array will be V (number of vertices
  // in graph)
  private int V;
  private List<LinkedList<Integer>> adjListArray;

  // constructor
  CommunityDetectionGraph(int V) {
    this.V = V;
    // define the size of array as
    // number of vertices
    adjListArray = new ArrayList<>(V);

    // Create a new list for each vertex
    // such that adjacent nodes can be stored

    for (int i = 0; i < V; i++) {
      adjListArray.add(new LinkedList<>());
    }
  }

  // Adds an edge to an undirected graph
  void addEdge(int src, int dest) {
    // Add an edge from src to dest.
    adjListArray.get(src).add(dest);

    // Since graph is undirected, add an edge from dest
    // to src also
    adjListArray.get(dest).add(src);
  }

  private void DFSUtil(int v, boolean[] visited, List<Integer> current) {
    // Mark the current node as visited and print it
    visited[v] = true;
    current.add(v);
    // Recur for all the vertices
    // adjacent to this vertex
    for (int x : adjListArray.get(v)) {
      if (!visited[x]) DFSUtil(x, visited, current);
    }

  }

  List<List<Integer>> connectedComponents() {
    // Mark all the vertices as not visited
    List<List<Integer>> connections = new ArrayList<>();
    boolean[] visited = new boolean[V];
    for (int v = 0; v < V; ++v) {
      if (!visited[v]) {
        List<Integer> current = new ArrayList<>();

        // print all reachable vertices
        // from v
        DFSUtil(v, visited, current);
        connections.add(current);

      }
    }
    return connections;
  }

  // Driver program to test above
  public static void main(String[] args) {
    // Create a graph given in the above diagram
    CommunityDetectionGraph g = new CommunityDetectionGraph(5); // 5 vertices numbered from 0 to 4

    g.addEdge(1, 0);
    g.addEdge(2, 3);
    g.addEdge(3, 4);
    // System.out.println("Following are connected components");
    List<List<Integer>> results = g.connectedComponents();
    results.stream().forEach(System.out::println);
  }

}
