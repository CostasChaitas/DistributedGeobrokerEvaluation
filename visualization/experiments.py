import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
sns.set(style="whitegrid")

df = pd.read_csv('./data/experiments.csv', sep=';')

#plot = sns.lmplot(x="Nodes", y="Clients", data=df, ci=None, scatter_kws={"s": 50})

plot = sns.lineplot(x="Nodes", y="Clients", data=df, marker='o')

plt.show()