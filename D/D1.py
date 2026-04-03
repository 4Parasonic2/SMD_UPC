"""
D1 — Lab part D, exercise 1
===========================
Implement the algorithm required for D1.

Options:
  • **Neo4j GDS** — ``pip install graphdatascience`` and use the GDS library
    (e.g. PageRank, shortest path) if your course uses it.
  • **Pure Cypher** — iterative queries or ``shortestPath`` / ``apoc`` if allowed.
  • **Python on exported data** — query Neo4j for subgraph, run algorithm locally.

Fill in ``run_algorithm`` below.
"""
def run_algorithm(graph):
    """
    Run the algorithm on the given graph.

    Parameters
    ----------
    graph : Graph
        The input graph on which to run the algorithm.

    Returns
    -------
    result
        The result of the algorithm.
    """