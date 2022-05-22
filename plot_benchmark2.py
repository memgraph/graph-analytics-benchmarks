from gqlalchemy import Memgraph
import pandas as pd
import datetime
from collections import defaultdict
import math
import seaborn as sns
from matplotlib import pyplot as plt

from yaml import load
sns.set_theme()
sns.set(font_scale = 1.2, font="Verdana")
plt.figure(figsize=(11, 8), 
           dpi = 600) 
# Create an array with the colors you want to use
colors = ["#2B7BB2", "#E66411", "#72B300"]
# Set your custom color palette
palette = sns.set_palette(sns.color_palette(colors))

SCALES = [
    2, 3, 5, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23
]
PAGERANK_ALG = {
    "networkx": "nxalg.pagerank()",
    "memgraph": "pagerank.get()",
    "cugraph": "cugraph.pagerank.get()"
}

data = pd.DataFrame()
for scale  in SCALES:
    for method in PAGERANK_ALG.keys():
        file_name = f"./benchmark/pagerank/{method}_{scale}.txt"
        if (method == 'networkx' and scale > 14) or (method == 'memgraph' and scale > 20):
            load_data = {}
            load_data['query_time'] = [math.inf]
            load_data['total_time'] = [math.inf]
            load_data['loading_time'] = [math.inf]
            load_data['execution_time'] = [math.inf]
            load_data = pd.DataFrame(data=load_data)
        else:
            load_data = pd.read_csv(file_name)

        if method == 'networkx':
            load_data['Method'] = 'NetworkX'
        elif method == 'cugraph':
            load_data['Method'] = 'cuGraph'
        else:
            load_data['Method'] = 'MAGE'
        load_data['scale'] = scale
        
        data = pd.concat([data, load_data])

data['scale'] = 2 ** data['scale']
data.reset_index(inplace=True)
# print(data)
# sns.lineplot(data=data,
# x="scale",
# y="execution_time",
# hue="Method",
# palette=palette,
# linewidth=3)
# plt.gca().set_ylim(0, 1000)
# plt.gca().set(xscale="log")
# plt.gca().set(ylabel="Execution time [ms]", xlabel="Number of vertices")
# plt.gca().set_title("PageRank - algorithm execution time")
# plt.savefig("execution_time_plot.png")


data['log_ex'] = [math.log10(i) for i in data['execution_time']]
print(data)
sns.lineplot(data=data,
x="scale",
y="execution_time",
hue="Method",
palette=palette,
linewidth=3)
plt.gca().set_ylim(0, 20)
plt.gca().set(xscale="log")
plt.gca().set(ylabel="Execution time [ms]", xlabel="Number of vertices")
plt.gca().set_title("PageRank - algorithm execution time")
plt.savefig("execution_time_log_plot.png")

plt.show()
