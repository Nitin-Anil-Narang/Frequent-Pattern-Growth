names.show(n=5, truncate=False)

actors=names.filter((f.array_contains(f.array(names.primaryProfession),"actor"))|(f.array_contains(f.array(names.primaryProfession),"actress")))
actors.show(n=10,truncate=False)
actors.select('primaryName').distinct().show()
actors.select('nconst').distinct().count()
basics.show(n=5, truncate=False)
basics.select('titleType').distinct().show()
basics_movies=basics.filter(basics.titleType=='movie')
basics_movies.show(n=10,truncate=False)
principals.show(n=5,truncate=False)
people= (
    names
    .withColumn("titles_array", f.split("primaryProfession", "\,"))
    .withColumn("primaryProfession", f.explode("titles_array"))
    .select("nconst", "primaryName","primaryProfession")
)
people.createOrReplaceTempView('people')
people.show()

query = """SELECT  primaryProfession AS profession, COUNT(*) AS people
           FROM people
           GROUP BY primaryProfession
           ORDER BY people DESC 
           """
people = spark.sql(query)
people.show()

people=people.toPandas()
people = people.set_index("profession")
people

pie = people.plot.pie(y = "people", figsize = (13,13), autopct = "%1.1f%%")
pie.legend(loc = "upper left");
pie.set(title = "people by professşon", ylabel = "Percentage of people");

movies=basics_movies.join(principals,basics_movies.tconst==principals.tconst).select(principals.tconst,basics_movies.primaryTitle,principals.nconst)

movies.show(n=10,truncate=False)

movies=movies.join(names,movies.nconst==names.nconst,"left").select(movies.tconst,movies.primaryTitle,movies.nconst,names.primaryName)

movies.cache()

movies.createOrReplaceTempView('movies')

movies.show(n=10,truncate=False)


import seaborn as sns
from wordcloud import WordCloud

query = """SELECT primaryName, COUNT(*) AS actor_count
           FROM movies
           GROUP BY primaryName
           ORDER BY actor_count desc
           LIMIT 10"""
appearance = spark.sql(query)
sns.set(rc={'figure.figsize':(10,6)})
ax = sns.barplot(x = "primaryName", y = "actor_count", data = appearance.toPandas())
ax.set(title = "Top 10 most appeared actors", xlabel = "Actor", ylabel = "Total Appearance")
ax.set_xticklabels(ax.get_xticklabels(), rotation=70);

Dict_ = {col:movies.filter(movies[col].isNull()).count() for col in movies.columns}
Dict_

Dict_ = {col:movies.filter(movies[col]=='\\N').count() for col in movies.columns}
Dict_

Dict_ = {col:movies.filter(movies[col]==' ').count() for col in movies.columns}
Dict_


print((movies.count(), len(movies.columns)))

movie_baskets=movies.groupBy('tconst').agg(f.collect_set('nconst').alias('actors'))

movie_baskets.show()

movie_baskets.printSchema()

movie_baskets.cache()

print(movie_baskets.count(), len(movie_baskets.columns))

basket_sizes=movie_baskets.withColumn("size", f.size("actors")).select('*')
basket_sizes.sort('size',ascending=False).show()

basket_sizes.groupBy('size').count().sort('count',ascending=False).show()

actor_movie_numbers=movies.groupBy('nconst').agg(f.collect_set('tconst').alias('movie_titles')).withColumn("size", f.size("movie_titles")).select('*').sort('size',ascending=False)

actor_movie_numbers.groupby('size').count().sort('count',ascending=False).show()