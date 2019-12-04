import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
sns.set(style="whitegrid")

def ecdf(data):
    """ Compute ECDF """
    x = np.sort(data)
    n = x.size
    y = np.arange(1, n+1) / n * 100
    return(x,y)


df1 = pd.read_csv('./data/latency/1node.csv', sep=';')
df3 = pd.read_csv('./data/latency/3nodes.csv', sep=';')
df5 = pd.read_csv('./data/latency/5nodes.csv', sep=';')


x1,y1 = ecdf(df1["Time(ms)"])
x3,y3 = ecdf(df3["Time(ms)"])
x5,y5 = ecdf(df5["Time(ms)"])


plot = sns.lineplot(x=x1, y=y1, hue=df1["Experiment"], legend="full", palette=['blue'])
plot = sns.lineplot(x=x3, y=y3, hue=df3["Experiment"], legend="full", palette=['orange'])
plot = sns.lineplot(x=x5, y=y5, hue=df5["Experiment"], legend="full", palette=['green'])


plot.set(xlabel='Latency (ms)')
plot.set(xlim=(0, 300))
y_value=['{:,.2f}'.format(x) + '%' for x in plot.get_yticks()]
plot.set_yticklabels(y_value)
plot.set(ylabel='ecdf (x)')

plot.get_figure().savefig("./output/latency.png", bbox_inches = "tight")

plt.show()