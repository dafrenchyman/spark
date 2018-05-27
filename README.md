# spark
Helpful Java Spark library.

Pipelines:
* ConcatColumns - When you need to concat a bunch of columns together
* DropColumns - Pipelines do get messy
* ExplodeVector - All those models sure like results in vectors
* FloorCeiling - For putting caps on your variables
* MultiStringIndexer - For the lazy
* TopCategories - When you want to limit the categorical variables
* XGBoostEstimator - Just using the Scala one, but added all the missing "sets" and "gets"

Params:
* MapParam - When you just need to use a Map

Misc:
* FindContinuousColumns - Dataset<Row> helper
* FindCategoricalColumns - Dataset<Row> helper
* FindNullColumns - Annoyingly, I have some datasets where columns can always be NULL.
* SmartUnion - For when your columns don't line up and the datasets don't always have the same ones.
* JavaToScala helpers
* SparkUnitTest helpers
* SparkSettings sysouts
* ...
