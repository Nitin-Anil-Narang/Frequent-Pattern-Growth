movie_baskets.toPandas().head(5)
import time
start_time = time.time()
fpGrowth = FPGrowth(itemsCol="actors", minSupport=0.0004, minConfidence=0)
model = fpGrowth.fit(movie_baskets)
model.freqItemsets.show(truncate=False)
end_time = time.time()
print('Duration: {}'.format(end_time - start_time))

# Display frequent itemsets.
mostPopularActorInABasket = model.freqItemsets
mostPopularActorInABasket.createOrReplaceTempView("mostPopularActorInABasket")
mostPopularActorInABasket.sort('freq', ascending=False).show(truncate=False)

# Display generated association rules.
associationRules = model.associationRules
associationRules.createOrReplaceTempView("associationRules")
associationRules.sort('confidence', ascending=False).show(truncate=False)

# transform examines the input items against all the association rules and summarize the consequents as prediction
associations = model.transform(movie_baskets)
associations.sort('prediction', ascending=False).show(truncate=False)

#Most frequent basket of items (containing at least 2 items)

query = """select items, freq
           from mostPopularActorInABasket
           where size(items) > 1
           order by freq desc
           limit 20"""
spark.sql(query).show(truncate=False)

#Association rules
query = """select antecedent as `antecedent (if)`, consequent as `consequent (then)`, confidence
           from associationRules
           order by confidence desc
           limit 20"""

spark.sql(query).show(truncate=False)