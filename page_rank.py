from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from graphframes import GraphFrame

sc = SparkContext("local", "PageRank")

spark = SparkSession(sc)

data = [
    (196, 242, 3),
    (186, 302, 3),
    (22, 377, 1),
    # Add more data here...
]

rdd = sc.parallelize(data)

df = spark.createDataFrame(rdd, ["user_id", "movie_id", "rating"])

edges_df = df.selectExpr("user_id as src", "movie_id as dst")

vertices_df = df.selectExpr("movie_id as id").distinct()

graph = GraphFrame(vertices_df, edges_df)

page_rank_results = graph.pageRank(resetProbability=0.15, tol=0.01)

page_rank_results.vertices.show()

sc.stop()
