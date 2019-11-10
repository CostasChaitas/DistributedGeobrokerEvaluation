import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
sns.set(style="whitegrid")

df = pd.read_csv('./data/latency.csv', sep=';')

plot = sns.boxplot(x="Experiment", y="Time(ms)", data=df)

means = df.groupby(['Experiment'])['Time(ms)'].mean().values
median_labels = [str(np.round(s, 2)) for s in means]

pos = range(len(means))
for tick,label in zip(pos,plot.get_xticklabels()):
    plot.text(pos[tick], means[tick] + 0.5, median_labels[tick],
            horizontalalignment='center', size='x-small', color='w', weight='semibold')


plot.set(ylim=(0, 100))

plt.show()