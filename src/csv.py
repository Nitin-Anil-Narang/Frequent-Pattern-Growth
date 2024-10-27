import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import pandas as pd
import numpy as np
import itertools
import matplotlib.pyplot as plt

sc=spark.sparkContext

names= spark.read.csv("/content/name.basics.tsv/name.basics.tsv",sep='\t', header=True, inferSchema=True)
basics=spark.read.csv("/content/title.basics.tsv/title.basics.tsv",sep='\t',header=True, inferSchema=True)
principals=spark.read.csv("/content/title.principals.tsv/title.principals.tsv",sep='\t',header=True, inferSchema=True)

names.createOrReplaceTempView("names")
basics.createOrReplaceTempView("basics")
principals.createOrReplaceTempView("principals")