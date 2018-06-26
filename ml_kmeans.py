import getopt
import sys
from pickle import load

import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler

def usage():
    print("Usage: python " + sys.argv[0] + " -m <measurement> -g <group>")
    print("    -i --input: ")
    print("    -c --clusters: ")
    print("    -h --help: Displays this text")

try:
    opts, args = getopt.getopt(sys.argv[1:], 'hi:c:', ["help", "input=", "clusters="])
except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(1)

input = ""
clusters = 3
for o, a in opts:
    if o in ('-i', '--input'):
        input = a
    elif o in ('-c', '--clusters'):
        clusters = int(a)
    elif o in ("-h", "--help"):
        usage()
        exit()

#if input:
#    with open(input) as file:
#        X = load(file)
#else:
#    X = load(sys.stdin)

X = [[1], [0.9994394618834082], [0.9994363021420518], [0.9994435169727324], [0.9989067055393587], [1], [1], [1], [1], [1], [1], [0.9955599407992107], [0.9938335046248715], [0.9966471081307627], [0.9985412107950401], [0.9997293640054127], [1], [0.9996900185988841], [0.9995883079456567], [1], [1], [1], [1], [0.999485596707819], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [0.999721293199554], [0.9997066588442359], [0.9994347088750706], [1], [0.9951338199513382], [0.9962740633409232], [0.997649271274095], [0.9986191659762497], [0.9994361432196223], [0.9994370954123276]]
y = [1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1]

data = []
for i,_ in enumerate(X[0]):
    data.append([x[i] for x in X])

#print data

#min_X = sys.maxint
#max_X = 0
#min_Y = sys.maxint
#max_Y = 0

#for i, value in enumerate(data[0]):
#    value2 = data[1][i]

#    if value < min_X:
#        min_X = value
#    if value > max_X:
#        max_X = value
#    if value2 < min_Y:
#        min_Y = value2
#    if value2 > max_Y:
#        max_Y = value2

    #X.append([value, value2])

data = MinMaxScaler().fit_transform(X)

reduced_data = PCA(n_components=1).fit_transform(data)

kmeans = KMeans(init='k-means++', n_clusters=2, n_init=10)
#kmeans.fit(reduced_data)
kmeans = KMeans(n_clusters=clusters).fit(reduced_data)
# Plot the decision boundary. For that, we will assign a color to each
x_min, x_max = reduced_data[:, 0].min() - 1, reduced_data[:, 0].max() + 1
y_min, y_max = reduced_data[:, 1].min() - 1, reduced_data[:, 1].max() + 1

h = 0.02

xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

# Obtain labels for each point in mesh. Use last trained model.
Z = kmeans.predict(np.c_[xx.ravel(), yy.ravel()])

# Put the result into a color plot
Z = Z.reshape(xx.shape)

# Put the result into a color plot
plt.figure(1)
plt.clf()
plt.imshow(Z, interpolation='nearest',
           extent=(xx.min(), xx.max(), yy.min(), yy.max()),
           cmap=plt.cm.Paired,
           aspect='auto', origin='lower')

#plt.plot(data[0], data[1], 'k.', markersize=2)
for i, label in enumerate(y):
    if label == 0:
        color = "ro"
    elif label == 1:
        color = "go"
    elif label == 2:
        color = "bo"

    plt.plot(reduced_data[i, 0], reduced_data[i, 1], color, markersize=4)
# Plot the centroids as a white X
centroids = kmeans.cluster_centers_
plt.scatter(centroids[:, 0], centroids[:, 1],
            marker='x', s=169, linewidths=3,
            color='w', zorder=10)
plt.xlim(x_min, x_max)
plt.ylim(y_min, y_max)
plt.xticks(())
plt.yticks(())
plt.show()
