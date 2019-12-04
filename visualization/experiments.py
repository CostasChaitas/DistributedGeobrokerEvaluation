import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
sns.set(style="whitegrid")

df = pd.read_csv('./data/experiments-clients.csv', sep=';')

plot = sns.lineplot(x="Nodes", y="Clients", data=df, marker='o')

plot.set(xticks=df.Nodes.values)

plot.get_figure().savefig("./output/evaluation-clients.png")

plt.show()

