# Databricks notebook source
#Year 2022
#Big Data Tools 2
#Team Members:  NITHARWAL Ashwani
#               ARQUESRUBIO Maria
#               HELLE Tristan

# COMMAND ----------

#path to access tables
path="/FileStore/tables/"

# COMMAND ----------

#to be used for transformation of data
pip install quinn

# COMMAND ----------

#importing necessary libraries
from pyspark.sql.functions import sum,avg,max,min,mean,count
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import *
import quinn
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.tuning import CrossValidator

# COMMAND ----------

# DBTITLE 1,Reading the data
### Read all of the files and display them to have an overview of the data ###

#parsed_business
file_location = path + "parsed_business.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
business = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

#parsed_checkin
file_location = path + "parsed_checkin.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
parsed_checkin = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

#parsed_covid
file_location = path + "parsed_covid.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
parsed_covid = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

#parsed_review
file_location = path + "parsed_review.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
parsed_review= spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------


#parsed_tip
file_location = path + "parsed_tip.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
parsed_tip= spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

#parsed_user
file_location = path + "parsed_user.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
parsed_user= spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

display(parsed_user)

# COMMAND ----------

display(business)

# COMMAND ----------

display(parsed_review)

# COMMAND ----------

display(parsed_covid)

# COMMAND ----------

display(parsed_checkin)

# COMMAND ----------

display(parsed_tip)

# COMMAND ----------

# DBTITLE 1,Parsed checkin processing
#from: https://sparkbyexamples.com/pyspark/pyspark-find-count-of-null-none-nan-values/
#to ccheck NAs in columns
from pyspark.sql.functions import col,isnan, when, count

parsed_checkin.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in parsed_checkin.columns]
   ).show()

# COMMAND ----------

checkin_grouped = parsed_checkin.groupBy("business_id").agg(count("date")).alias("number of count")

# COMMAND ----------

# DBTITLE 1,Parsed tip processing
#to check NAs in columns
parsed_tip.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in parsed_tip.columns]
   ).show()

# COMMAND ----------

#getting compliment counts per business id
tips_grouped = parsed_tip.groupBy("business_id") \
.agg(sum("compliment_count").alias("compliment_count"), \
countDistinct("date").alias("tips_count"))

# COMMAND ----------

# DBTITLE 1,Parsed user processing
#checking null values in the columns
parsed_user.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in parsed_user.columns]
   ).show()

# COMMAND ----------

# DBTITLE 1,Parsed business processing


# COMMAND ----------

#to replace column names from dots to underscore
def dots_to_underscores(s):
    return s.replace(".", "_")
business = quinn.with_columns_renamed(dots_to_underscores)(business)
business.show()

# COMMAND ----------

#to check NAs in every column
from pyspark.sql.functions import isnan, when, count, col
display(business.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in business.columns]))

# COMMAND ----------

#to drop columns having more than 30% of null values (all the attribute columns)
cols = 'attributes_AcceptsInsurance', 'attributes_AgesAllowed', 'attributes_Alcohol', 'attributes_Ambience', 'attributes_BYOB', 'attributes_BYOBCorkage', 'attributes_BestNights', 'attributes_BikeParking', 'attributes_BusinessAcceptsBitcoin', 'attributes_BusinessAcceptsCreditCards', 'attributes_BusinessParking', 'attributes_ByAppointmentOnly', 'attributes_Caters', 'attributes_CoatCheck', 'attributes_Corkage', 'attributes_DietaryRestrictions', 'attributes_DogsAllowed', 'attributes_DriveThru', 'attributes_GoodForDancing', 'attributes_GoodForKids', 'attributes_GoodForMeal', 'attributes_HairSpecializesIn', 'attributes_HappyHour', 'attributes_HasTV', 'attributes_Music', 'attributes_NoiseLevel', 'attributes_Open24Hours', 'attributes_OutdoorSeating', 'attributes_RestaurantsAttire', 'attributes_RestaurantsCounterService', 'attributes_RestaurantsDelivery', 'attributes_RestaurantsGoodForGroups', 'attributes_RestaurantsPriceRange2', 'attributes_RestaurantsReservations', 'attributes_RestaurantsTableService', 'attributes_RestaurantsTakeOut', 'attributes_Smoking', 'attributes_WheelchairAccessible', 'attributes_WiFi'

business_new = business.drop(*cols)

# COMMAND ----------

display(business_new)

# COMMAND ----------

#to check null values in columns
display(business_new.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in business_new.columns]
   ))

# COMMAND ----------

#to fill null values of categories column with most used word 'Restaurant'
business_new = business_new.na.fill("Restaurant",["categories"])

# COMMAND ----------

business_new.createOrReplaceTempView("sql_business")
#getting most occured timimngs of the businesses and respectively filling the null values with them on the days
h = spark.sql("select hours_Friday from sql_business where hours_Friday is not null group by 1 order by count(*) desc limit 1").collect()[0][0]
business_new = business_new.na.fill(h,["hours_Friday"])
h = spark.sql("select hours_Monday from sql_business where hours_Monday is not null group by 1 order by count(*) desc limit 1").collect()[0][0]
business_new = business_new.na.fill(h,["hours_Monday"])
h = spark.sql("select hours_Saturday from sql_business where hours_Saturday is not null group by 1 order by count(*) desc limit 1").collect()[0][0]
business_new = business_new.na.fill(h,["hours_Saturday"])
h = spark.sql("select hours_Sunday from sql_business where hours_Sunday is not null group by 1 order by count(*) desc limit 1").collect()[0][0]
business_new = business_new.na.fill(h,["hours_Sunday"])
h = spark.sql("select hours_Thursday from sql_business where hours_Thursday is not null group by 1 order by count(*) desc limit 1").collect()[0][0]
business_new = business_new.na.fill(h,["hours_Thursday"])
h = spark.sql("select hours_Tuesday from sql_business where hours_Tuesday is not null group by 1 order by count(*) desc limit 1").collect()[0][0]
business_new = business_new.na.fill(h,["hours_Tuesday"])
h = spark.sql("select hours_Wednesday from sql_business where hours_Wednesday is not null group by 1 order by count(*) desc limit 1").collect()[0][0]
business_new = business_new.na.fill(h,["hours_Wednesday"])

# COMMAND ----------

#checking NAs again 
display(business_new.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in business_new.columns]
   ))

# COMMAND ----------

#filtering the rows and keeping only few keyword categories like eatables, food and drinks
business_new = business_new.filter((col("categories").contains("Restaurants")) | (col("categories").contains("Food")) | (col("categories").contains("Grocery")) | (col("categories").contains("Bars"))\
                                  | (col("categories").contains("Coffe"))\
                                  | (col("categories").contains("Tea"))\
                                  | (col("categories").contains("Bakeries"))\
                                  | (col("categories").contains("Steakhouse"))\
                                  | (col("categories").contains("Noodles"))\
                                  | (col("categories").contains("Cafes"))\
                                  | (col("categories").contains("Vegetarian"))\
                                  | (col("categories").contains("Seafood"))\
                                  | (col("categories").contains("Breakfast"))\
                                  | (col("categories").contains("Seafood"))\
                                  | (col("categories").contains("Shopping"))\
                                  | (col("categories").contains("Brunch")))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Parsed covid processing
#checking NAs
display(parsed_covid.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in parsed_covid.columns]))

# COMMAND ----------

#checking NAs
parsed_covidNA = parsed_covid.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in parsed_covid.columns])

parsed_covidNA.show()

# COMMAND ----------

#preview of data
display(parsed_covid)

# COMMAND ----------

# % of missing values
1 - ((parsed_covid.count()-21)/parsed_covid.count()) 

# COMMAND ----------

#replacing values with binary digits
from pyspark.sql import functions as F

covid_new = parsed_covid.withColumn('Covid Banner',(F.when(col('Covid Banner') == 'FALSE', 0).otherwise(1)))
covid_new = covid_new.withColumn('Highlights',(F.when(col('Highlights') == 'FALSE', 0).otherwise(1)))
covid_new = covid_new.withColumn('Virtual Services Offered',(F.when(col('Virtual Services Offered') == 'FALSE', 0).otherwise(1))) #We assign 1 to businesses that offer classes / consultation / tours
covid_new = covid_new.withColumn('Temporary Closed Until',(F.when(col('Temporary Closed Until') == 'FALSE', 0).otherwise(1))) # 1 is assigned to the businesses that were temporarly closed in june & july
covid_new = covid_new.withColumn('Call To Action enabled',(F.when(col('Call To Action enabled') == 'FALSE', 0).otherwise(1)))
covid_new = covid_new.withColumn('Grubhub enabled',(F.when(col('Grubhub enabled') == 'FALSE', 0).otherwise(1)))
covid_new = covid_new.withColumn('Request a Quote Enabled',(F.when(col('Request a Quote Enabled') == 'FALSE', 0).otherwise(1)))
covid_new = covid_new.withColumn('delivery or takeout',(F.when(col('delivery or takeout') == 'FALSE', 0).otherwise(1)))

# COMMAND ----------

display(covid_new)

# COMMAND ----------

df2=parsed_covid.select(countDistinct("business_id"))

# COMMAND ----------

#exact number of businesses
display(df2)

# COMMAND ----------

#application of the groupby in order to have one row per business_id
covid_grouped = covid_new.groupBy("business_id") \
.agg(sum("Call to Action enabled").alias("Call to Action enabled"), \
sum("Covid Banner").alias("Covid Banner"), \
sum("Grubhub enabled").alias("Grubhub enabled"), \
sum("Request a Quote Enabled").alias("Request a Quote Enabled"), \
sum("Temporary Closed Until").alias("Temporary Closed Until"), \
sum("Virtual Services Offered").alias("Virtual Services Offered"), \
sum("delivery or takeout").alias("delivery or takeout"), \
sum("Highlights").alias("Highlights"))




# COMMAND ----------

display(covid_grouped)

# COMMAND ----------

# DBTITLE 1,Parsed review processing
#checking NAs
parsed_reviewNA = parsed_review.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in parsed_review.columns])

parsed_reviewNA.show()

# COMMAND ----------

#application of the groupby in order to have one row per business_id
review_grouped=parsed_review.groupBy("business_id") \
.agg(sum("cool").alias("number_of_cool_votes"), \
sum("funny").alias("number_of_funny_votes"), \
sum("useful").alias("number_of_useful_votes"), \
avg("stars").alias("avg_stars"), \
countDistinct("user_id").alias("number_user_id"),\
countDistinct("review_id").alias("number_reviews"))

# COMMAND ----------

# DBTITLE 1,Merging for the basetable
### Here we merge all of the needed tables to have a final observation for each of the businesses

# COMMAND ----------

business_tip_gp = business_new.join(tips_grouped, on=['business_id'], how='left')

# COMMAND ----------

business_tip_checkin_gp = business_tip_gp.join(checkin_grouped, on=['business_id'], how='left')

# COMMAND ----------

business_tip_checkin_covid_gp = business_tip_checkin_gp.join(covid_grouped, on=['business_id'], how='left')

# COMMAND ----------

business_tip_checkin_covid_review_gp = business_tip_checkin_covid_gp.join(review_grouped, on=['business_id'], how='left')

# COMMAND ----------

df = business_tip_checkin_covid_review_gp

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#Create the open hours per each day of the week (does not take into account if the building is 24/7 open)

#from: https://sparkbyexamples.com/pyspark/pyspark-split-dataframe-column-into-multiple-columns/

from pyspark.sql.functions import split
from pyspark.sql.types import IntegerType
 
df = df.withColumn('hF_morning', split(df['hours_Friday'], '-').getItem(0)) \
       .withColumn('hF_evening', split(df['hours_Friday'], '-').getItem(1))

df1 = df.withColumn('hF_morning', split(df['hF_morning'], ':').getItem(0)) \
       .withColumn('hF_evening', split(df['hF_evening'], ':').getItem(0))

df1 = df1.withColumn("hours_open_friday", abs(df1['hF_evening'].cast(IntegerType()) - df1['hF_morning'].cast(IntegerType())))

df1 = df1.withColumn('hF_morning', split(df1['hours_Monday'], '-').getItem(0)) \
       .withColumn('hF_evening', split(df1['hours_Monday'], '-').getItem(1))

df1 = df1.withColumn('hF_morning', split(df1['hF_morning'], ':').getItem(0)) \
       .withColumn('hF_evening', split(df1['hF_evening'], ':').getItem(0))

df1 = df1.withColumn("hours_open_monday", abs(df1['hF_evening'].cast(IntegerType()) - df1['hF_morning'].cast(IntegerType())))

df1 = df1.withColumn('hF_morning', split(df1['hours_Saturday'], '-').getItem(0)) \
       .withColumn('hF_evening', split(df1['hours_Saturday'], '-').getItem(1))

df1 = df1.withColumn('hF_morning', split(df1['hF_morning'], ':').getItem(0)) \
       .withColumn('hF_evening', split(df1['hF_evening'], ':').getItem(0))

df1 = df1.withColumn("hours_open_saturday", abs(df1['hF_evening'].cast(IntegerType()) - df1['hF_morning'].cast(IntegerType())))

df1 = df1.withColumn('hF_morning', split(df1['hours_Sunday'], '-').getItem(0)) \
       .withColumn('hF_evening', split(df1['hours_Sunday'], '-').getItem(1))

df1 = df1.withColumn('hF_morning', split(df1['hF_morning'], ':').getItem(0)) \
       .withColumn('hF_evening', split(df1['hF_evening'], ':').getItem(0))

df1 = df1.withColumn("hours_open_sunday", abs(df1['hF_evening'].cast(IntegerType()) - df1['hF_morning'].cast(IntegerType())))

df1 = df1.withColumn('hF_morning', split(df1['hours_Thursday'], '-').getItem(0)) \
       .withColumn('hF_evening', split(df1['hours_Thursday'], '-').getItem(1))

df1 = df1.withColumn('hF_morning', split(df1['hF_morning'], ':').getItem(0)) \
       .withColumn('hF_evening', split(df1['hF_evening'], ':').getItem(0))

df1 = df1.withColumn("hours_open_thursday", abs(df1['hF_evening'].cast(IntegerType()) - df1['hF_morning'].cast(IntegerType())))

df1 = df1.withColumn('hF_morning', split(df1['hours_Tuesday'], '-').getItem(0)) \
       .withColumn('hF_evening', split(df1['hours_Tuesday'], '-').getItem(1))

df1 = df1.withColumn('hF_morning', split(df1['hF_morning'], ':').getItem(0)) \
       .withColumn('hF_evening', split(df1['hF_evening'], ':').getItem(0))

df1 = df1.withColumn("hours_open_tuesday", abs(df1['hF_evening'].cast(IntegerType()) - df1['hF_morning'].cast(IntegerType())))

df1 = df1.withColumn('hF_morning', split(df1['hours_wednesday'], '-').getItem(0)) \
       .withColumn('hF_evening', split(df1['hours_wednesday'], '-').getItem(1))

df1 = df1.withColumn('hF_morning', split(df1['hF_morning'], ':').getItem(0)) \
       .withColumn('hF_evening', split(df1['hF_evening'], ':').getItem(0))

df1 = df1.withColumn("hours_open_wednesday", abs(df1['hF_evening'].cast(IntegerType()) - df1['hF_morning'].cast(IntegerType())))


df1 = df1.drop('hF_morning','hF_evening')

# COMMAND ----------

#creating variables for a restaurant or food availabilty
df1 = df1.withColumn("is_food_restaurant", when(col("categories").contains("Food"),1)
      .when(col("categories").contains("Restaurant") ,1)
      .otherwise(0))

# COMMAND ----------

#creating variables for availability of drinks in a restaurant
df1 = df1.withColumn("is_drinks_restaurant", when(col("categories").contains("Coffe"),1)
      .when(col("categories").contains("Tea") ,1)
      .when(col("categories").contains("Juice") ,1)
      .when(col("categories").contains("Cocktails") ,1)
      .otherwise(0))

# COMMAND ----------

#creating variable 1 if it's open on that day else 0
df1 = df1.withColumn("is_open_monday", when(col("hours_open_monday") > 0 ,1).otherwise(0))

df1 = df1.withColumn("is_open_tuesday", when(col("hours_open_tuesday") > 0 ,1).otherwise(0))

df1 = df1.withColumn("is_open_wednesday", when(col("hours_open_wednesday") > 0 ,1).otherwise(0))

df1 = df1.withColumn("is_open_thursday", when(col("hours_open_thursday") > 0 ,1).otherwise(0))

df1 = df1.withColumn("is_open_friday", when(col("hours_open_friday") > 0 ,1).otherwise(0))

df1 = df1.withColumn("is_open_saturday", when(col("hours_open_saturday") > 0 ,1).otherwise(0))

df1 = df1.withColumn("is_open_sunday", when(col("hours_open_sunday") > 0 ,1).otherwise(0))

df1 = df1.withColumn("open_days_week", col("is_open_monday") + col("is_open_tuesday") + col("is_open_wednesday") + col("is_open_thursday") + col("is_open_friday") + col("is_open_saturday") + col("is_open_sunday"))

# COMMAND ----------

df1.display(3)

# COMMAND ----------

#changing data type of column
df1 = df1.withColumn("hours_open_friday", df1["hours_open_friday"].cast(IntegerType()))
df1 = df1.withColumn("hours_open_monday", df1["hours_open_monday"].cast(IntegerType()))
df1 = df1.withColumn("hours_open_saturday", df1["hours_open_saturday"].cast(IntegerType()))
df1 = df1.withColumn("hours_open_sunday", df1["hours_open_sunday"].cast(IntegerType()))
df1 = df1.withColumn("hours_open_tuesday", df1["hours_open_tuesday"].cast(IntegerType()))
df1 = df1.withColumn("hours_open_wednesday", df1["hours_open_wednesday"].cast(IntegerType()))
df1 = df1.withColumn("hours_open_thursday", df1["hours_open_thursday"].cast(IntegerType()))

# COMMAND ----------

#dropping variables which are not relevant to models - name / longitude / latitude / name / postal code 

df1= df1.drop("name",'longitude','latitude','postal_code','address')


# COMMAND ----------

df1.display()

# COMMAND ----------

#changing name 
df1 = df1.withColumnRenamed("delivery or takeout", "delivery_or_takeout")

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# DBTITLE 1,Model Creating & Machine Learning
#finalizing basetable 
basetable_final = df1

# COMMAND ----------

#checking NAs
from pyspark.sql.functions import isnan, when, count, col
display(basetable_final.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in basetable_final.columns]))

# COMMAND ----------

#filling NAs with 0
basetable_final = basetable_final.fillna(value=0)

# COMMAND ----------

#checking NAs
display(basetable_final.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in basetable_final.columns]))

# COMMAND ----------

#changing target column name to label 
basetable_final = basetable_final.withColumnRenamed("delivery_or_takeout","label")

# COMMAND ----------

basetable_final.printSchema()

# COMMAND ----------

#to create int index value for string columns unique values to be processed in models
from pyspark.ml.feature import StringIndexer

#ibusiness_id = StringIndexer(inputCol="business_id", outputCol="business_id_index", handleInvalid="skip")
icategories = StringIndexer(inputCol="categories", outputCol="categories_index", handleInvalid="skip")
icity = StringIndexer(inputCol="city", outputCol="city_index", handleInvalid="skip")
ihours_Monday = StringIndexer(inputCol="hours_Monday", outputCol="hours_Monday_index", handleInvalid="skip")
ihours_Saturday= StringIndexer(inputCol="hours_Saturday", outputCol="hours_Saturday_index", handleInvalid="skip")
ihours_Friday= StringIndexer(inputCol="hours_Friday", outputCol="hours_Friday_index", handleInvalid="skip")
ihours_Sunday= StringIndexer(inputCol="hours_Sunday", outputCol="hours_Sunday_index", handleInvalid="skip")
ihours_Thursday= StringIndexer(inputCol="hours_Thursday", outputCol="hours_Thursday_index", handleInvalid="skip")
ihours_Tuesday= StringIndexer(inputCol="hours_Tuesday", outputCol="hours_Tuesday_index", handleInvalid="skip")
ihours_Wednesday= StringIndexer(inputCol="hours_Wednesday", outputCol="hours_Wednesday_index", handleInvalid="skip")
istate= StringIndexer(inputCol="state", outputCol="state_index", handleInvalid="skip")

# COMMAND ----------

#assigning that index to all values in that column
from pyspark.ml.feature import OneHotEncoder

oneHotEnc = OneHotEncoder(
  outputCols=["city_v", "categories_v", "hours_Monday_v", "hours_Saturday_v","hours_Friday_v","hours_Sunday_v", "hours_Thursday_v", "hours_Tuesday_v", "hours_Wednesday_v", "state_v"],
  inputCols=[ "city_index","categories_index", "hours_Monday_index", "hours_Saturday_index","hours_Friday_index","hours_Sunday_index", "hours_Thursday_index", "hours_Tuesday_index", "hours_Wednesday_index", "state_index"]
)

# COMMAND ----------

#all the transformed or numerical columns to be used in models
featureCols = ['is_open','review_count','stars','compliment_count','tips_count','count(date)','Call to Action enabled','Covid Banner','Grubhub enabled','Request a Quote Enabled','Temporary Closed Until','Virtual Services Offered', 'Highlights','number_of_cool_votes','number_of_funny_votes','number_of_useful_votes','avg_stars','number_user_id','number_reviews','hours_open_friday','hours_open_monday', 'hours_open_saturday', 'hours_open_sunday', 'hours_open_thursday', 'hours_open_tuesday', 'hours_open_wednesday', 'is_food_restaurant', 'is_drinks_restaurant', 'is_open_monday', 'is_open_tuesday', 'is_open_wednesday', 'is_open_thursday', 'is_open_friday', 'is_open_saturday', 'is_open_sunday', 'open_days_week',"city_v", "categories_v", "hours_Monday_v", "hours_Saturday_v","hours_Friday_v","hours_Sunday_v", "hours_Thursday_v", "hours_Tuesday_v", "hours_Wednesday_v", "state_v"
]

# COMMAND ----------

#to create a vector for all features in a single column for models to work
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=featureCols, outputCol="features")

# COMMAND ----------

#pipeline defined to process all the above steps in a pipeline
from pyspark.ml import Pipeline

featurizationPipeline = Pipeline(stages=[icity,
  icategories,ihours_Monday,ihours_Saturday,ihours_Friday,ihours_Sunday,ihours_Thursday,ihours_Tuesday,ihours_Wednesday,istate,oneHotEnc,assembler])

# COMMAND ----------

#splitting the dataset into 2:8 ratio of test and train
seed = 42
(test, train) = basetable_final.randomSplit((0.20, 0.80), seed=seed)

print(test.count(), train.count())

# COMMAND ----------

# DBTITLE 1,Decision Tree Classifier
#Classification Model 1
from pyspark.ml.classification import DecisionTreeClassifier

dt = DecisionTreeClassifier().setLabelCol('label')
piplineFull = Pipeline(stages=[featurizationPipeline,dt])

#fitting model
b = piplineFull.fit(train)

# COMMAND ----------

#oredicting values for test data
predictionsDFtest = b.transform(test)

# COMMAND ----------

#preview of predictions made by decision tree classifier
display(predictionsDFtest)

# COMMAND ----------

#to calculate AUC on predictions on test data
from pyspark.ml.evaluation import BinaryClassificationEvaluator
my_eval_dt = BinaryClassificationEvaluator(rawPredictionCol='prediction')
predictionsDFtest.select('label','prediction')
AUC_dt = my_eval_dt.evaluate(predictionsDFtest)
print("AUC score on test data is : ",AUC_dt)

# COMMAND ----------

#predicting on train data to check the model
predictionsDF = b.transform(train)

# COMMAND ----------

#To check the predictions on train data
display(predictionsDF)

# COMMAND ----------

#To calculate AUC on train data set prediction
predictionsDF = predictionsDF.withColumnRenamed("delivery_or_takeout","label")
from pyspark.ml.evaluation import BinaryClassificationEvaluator
my_eval_dt = BinaryClassificationEvaluator(rawPredictionCol='prediction')
predictionsDFtest.select('label','prediction')
AUC_dt = my_eval_dt.evaluate(predictionsDF)
print("AUC score of Train data is : ",AUC_dt)

# COMMAND ----------

#to get important feature / impactful features
b.stages[-1].featureImportances

# COMMAND ----------

#function to extract important features
#code from www.timlrx.com/blog/
def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    return(varlist.sort_values('score', ascending = False))

# COMMAND ----------

#implementation above function and get important features
from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorSlicer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors

import pandas as pd
#selecting top 20 features as important
varlist = ExtractFeatureImp(b.stages[-1].featureImportances, predictionsDF, "features")
varidx20 = [x for x in varlist['idx'][0:20]]

slicer20 = VectorSlicer(inputCol="features", outputCol="features2", indices=varidx20)
#implement only important features extracted as new features
trainDF = slicer20.transform(predictionsDF)
testDF = slicer20.transform(predictionsDFtest)
#dropping previous predictions to perform new predictions
trainDF = trainDF.drop('rawPrediction', 'probability', 'prediction')
testDF = testDF.drop('rawPrediction', 'probability', 'prediction')
#changing features column to new and selected variables only
dt = DecisionTreeClassifier().setFeaturesCol("features2")

#fitting new model with reduced features to reduce model complexity
reduced20Model = dt.fit(trainDF)
#predicting using reduced features
pred_reduced20_model = reducedModel.transform(testDF)


# COMMAND ----------

#To calculate AUC score of reduced model with top 20 important features only
from pyspark.ml.evaluation import BinaryClassificationEvaluator
my_eval_dt = BinaryClassificationEvaluator(rawPredictionCol='prediction')
pred_reduced20_model.select('label','prediction')
AUC_dt = my_eval_dt.evaluate(pred_reduced20_model)
print("AUC score of reduced feature model is : ",AUC_dt)

# COMMAND ----------

# to repeat the decision tree with top 10 most important features
from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorSlicer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors

import pandas as pd
#taking 10 most important features
varlist = ExtractFeatureImp(b.stages[-1].featureImportances, predictionsDF, "features")
varidx = [x for x in varlist['idx'][0:10]]

slicer = VectorSlicer(inputCol="features", outputCol="features2", indices=varidx)
trainDF = slicer.transform(predictionsDF)
testDF = slicer.transform(predictionsDFtest)
trainDF = trainDF.drop('rawPrediction', 'probability', 'prediction')
testDF = testDF.drop('rawPrediction', 'probability', 'prediction')
dt = DecisionTreeClassifier().setFeaturesCol("features2")

#fitting model with top 10 important features only
reducedModel = dt.fit(trainDF)
#predictions based on top 10 features
pred_reduced_model = reducedModel.transform(testDF)


# COMMAND ----------

#overview of prediction on reduced and less complex model
display(pred_reduced_model)

# COMMAND ----------

#To calculate AUC of model with 10 most important features
from pyspark.ml.evaluation import BinaryClassificationEvaluator
my_eval_dt = BinaryClassificationEvaluator(rawPredictionCol='prediction')
pred_reduced_model.select('label','prediction')
AUC_dt = my_eval_dt.evaluate(pred_reduced_model)
print("AUC score of reduced feature model is : ",AUC_dt)

# COMMAND ----------

# DBTITLE 1,Logistic Regression
#Model 2
#Train a Logistic Regression model
from pyspark.ml.classification import LogisticRegression

#Define the algorithm class
lr = LogisticRegression().setLabelCol('label')
lr_pipeline = Pipeline(stages=[featurizationPipeline,lr])

#Fit the model
lrModel = lr_pipeline.fit(train)

# COMMAND ----------

#predictions on test data
lr_predictionsDFtest = lrModel.transform(test)

# COMMAND ----------

display(lr_predictionsDFtest)

# COMMAND ----------

#Calculting AUC score of Logistic Regression on test data
from pyspark.ml.evaluation import BinaryClassificationEvaluator
my_eval_dt = BinaryClassificationEvaluator(rawPredictionCol='prediction')
lr_predictionsDFtest.select('label','prediction')
AUC_dt = my_eval_dt.evaluate(lr_predictionsDFtest)
print("AUC score on test data is : ",AUC_dt)

# COMMAND ----------

#Calculting AUC score of Logistic Regression on train data
lr_predictionsDF = lrModel.transform(train)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
my_eval_dt = BinaryClassificationEvaluator(rawPredictionCol='prediction')
lr_predictionsDF.select('label','prediction')
AUC_dt = my_eval_dt.evaluate(lr_predictionsDF)
print("AUC score on train data is : ",AUC_dt)

# COMMAND ----------

#Finalizing Decision Tree Classifier model as the best model as it gets the best AUC score
#Decision Tree have AUC score in the order 
#        model with all features > model with 10 important features > model with top 20 features
#Hence to make model less complex, we selected:
#
#                                        decision tree classifier with 10 most important features

# COMMAND ----------

#top 10 important features selected
featureCols = [x for x in varlist['name'][0:10]]
featureCols

# COMMAND ----------

#function to get accuracy and other evaluation value for the predictions with plotting of ROC curve
# basic code from www.kaggle.com/code/anuragsharma0/ and later customized
def getEvaluationMatrix(predicDF):
    from  pyspark.mllib.evaluation import BinaryClassificationMetrics
    lablePrediction = predicDF.select( "label", "prediction")
    lablePrediction.cache()
    totalCount = lablePrediction.count()
    correctCount = lablePrediction.filter(col("label") == col("prediction")).count()
    wrongCount = lablePrediction.filter(~(col("label") == col("prediction"))).count()
    trueP = lablePrediction.filter(col("label") == 0.0).filter(col("label") == col("prediction")).count()
    trueN = lablePrediction.filter(col("label") == 1.0).filter(col("label") == col("prediction")).count()
    falseN = lablePrediction.filter(col("label") == 1.0).filter(~(col("label") == col("prediction"))).count()
    falseP = lablePrediction.filter(col("label") == 0.0).filter(~(col("label") == col("prediction"))).count()

    ratioWrong = float(wrongCount) / float(totalCount) 
    ratioCorrect = float(correctCount)/ float(totalCount)

    print("totalCount   - ", totalCount)
    print("correctCount - ", correctCount)
    print("wrongCount   - ", wrongCount)
    print("trueP        - ", trueP)
    print("trueN        - ", trueN)
    print("falseN       - ", falseN)
    print("falseP       - ", falseP)
    print("ratioWrong   - ", ratioWrong)
    print("ratioCorrect - ", ratioCorrect)
    
    precision = ((float(trueP) / (float(trueP) + float(falseP))) * 100 )
    recall = ((float(trueP) / (float(trueP) + float(falseN))) * 100 )
    print("Accuracy     - ", (trueP + trueN) / totalCount)
    print("Precision    - ", precision)
    print("Recall       - ", recall)
    print("F-1 Score    - ", ((2* ( (precision*recall) / (precision + recall))) ))
    print("Sensitivity  - ", ((float(trueP) / (float(trueP) + float(falseN))) * 100 ))
    print("Specificity  - ", ((float(trueN) / (float(trueN) + float(falseP))) * 100 ))
    
    createROC(predicDF)

# ### Method to compute the ROC Curve
def createROC(predictions):
    from sklearn.metrics import roc_curve, auc
    from  pyspark.mllib.evaluation import BinaryClassificationMetrics
    import matplotlib.pyplot as plt
    results = predictions.select(['probability', 'label'])
 
    ## prepare score-label set
    results_collect = results.collect()
    results_list = [(float(i[0][0]), 1.0-float(i[1])) for i in results_collect]
    scoreAndLabels = spark.sparkContext.parallelize(results_list)
 
    bcMetrics = BinaryClassificationMetrics(scoreAndLabels)
    print("ROC score is - ", bcMetrics.areaUnderROC)
        
    fpr = dict()
    tpr = dict()
    roc_auc = dict()
 
    y_test = [i[1] for i in results_list]
    y_score = [i[0] for i in results_list]
 
    fpr, tpr, _ = roc_curve(y_test, y_score)
    roc_auc = auc(fpr, tpr)
 
    plt.figure()
    plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic example')
    plt.legend(loc="lower right")
    plt.show()

# COMMAND ----------

#Evaluation of Logistic Regression model and AUC ROC curve
getEvaluationMatrix(lr_predictionsDFtest)

# COMMAND ----------

#Evaluation of Decision Tree Classifier model with all features and AUC ROC curve 
getEvaluationMatrix(predictionsDFtest)

# COMMAND ----------

#Evaluation of Decision Tree Classifier model with best 20 features and AUC ROC curve
getEvaluationMatrix(pred_reduced20_model)

# COMMAND ----------

#Evaluation of Decision Tree Classifier model with top 10 features (selected model) and AUC ROC curve
getEvaluationMatrix(pred_reduced_model)

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

paramGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [2, 4, 6])
             .addGrid(dt.maxBins, [20, 60])
             .build())
dt = DecisionTreeClassifier().setFeaturesCol("features")
evaluator = BinaryClassificationEvaluator()
piplineFull = Pipeline(stages=[featurizationPipeline])
piplineFull.fit(train)
cv = CrossValidator(estimator=dt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# Run cross validations.  This can take about 6 minutes since it is training over 20 trees!
cvModel = cv.fit(train)
predictions = cvModel.transform(test)
evaluator.evaluate(predictions)

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

paramGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [2, 4, 6])
             .addGrid(dt.maxBins, [20, 60])
             .build())

cv = CrossValidator(estimator=dt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=20)

# Run cross validations.  This can take about 6 minutes since it is training over 20 trees!
cvModel2 = cv.fit(trainDF)
predictions2 = cvModel2.transform(testDF)
evaluator.evaluate(predictions2)
