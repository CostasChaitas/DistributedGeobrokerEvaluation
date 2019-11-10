import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
sns.set(style="whitegrid")

df = pd.read_csv('./data/cpu.csv', sep=';')

plot = sns.lineplot(x="Time(s)", y="CPU(%)", hue="Clients",  legend="full", data=df)

plt.show()