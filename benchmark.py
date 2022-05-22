from collections import defaultdict
from itertools import product
import datetime
import math

from gqlalchemy import Memgraph
import pandas as pd

memgraph = Memgraph(port=7680)

ALGORITHMS = ("pagerank", "betweenness_centrality")
SCALES = (2, 3, 5, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)
LIBRARIES = ("cuGraph", "MAGE", "NetworkX")
N_EXECUTIONS = 20

CALLS = {
    "pagerank": {
        "cuGraph": "cugraph.pagerank.get()",
        "MAGE": "pagerank.get()",
        "NetworkX": "nxalg.pagerank()",
    },
    "betweenness_centrality": {
        "cuGraph": "cugraph.betweenness_centrality.get()",
        "MAGE": "betweenness_centrality.get()",
        "NetworkX": "nxalg.betweenness_centrality()",
    },
}


def load_dataset(scale, n_edges):
    memgraph.execute(
        """
        MATCH (n) DETACH DELETE n;
        """
    )
    memgraph.execute(
        f"""
        CALL cugraph.generator.rmat({scale}, {n_edges}) YIELD *;
        """
    )


def execute(algorithm, library):
    query = f"""
    CALL {CALLS[algorithm][library]} YIELD *
    RETURN loading_time, execution_time, total_time
    """

    start_time = datetime.datetime.now()
    result = list(memgraph.execute_and_fetch(query))[0]
    query_time = (datetime.datetime.now() - start_time).total_seconds() * 1000

    return (
        query_time,
        result["total_time"],
        result["loading_time"],
        result["execution_time"],
    )


def benchmark():
    for (algorithm, scale) in product(ALGORITHMS, SCALES):
        n_edges = 3 * 2**scale
        load_dataset(scale, n_edges)

        for library in LIBRARIES:
            path = f"./output/{algorithm}/{library}_{scale}.txt"

            data = defaultdict(list)

            if library == "NetworkX" and scale > 13:
                data["query_time"].append(math.inf)
                data["total_time"].append(math.inf)
                data["loading_time"].append(math.inf)
                data["execution_time"].append(math.inf)
                continue

            for _ in range(N_EXECUTIONS):
                query_time, total_time, loading_time, execution_time = execute(
                    algorithm, library
                )
                data["query_time"].append(query_time)
                data["total_time"].append(total_time)
                data["loading_time"].append(loading_time)
                data["execution_time"].append(execution_time)

            pd.DataFrame(data=data).to_csv(path, index=False)


if __name__ == "__main__":
    benchmark()
