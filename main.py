from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
import functions

spark = SparkSession.builder.appName('CS436_Project').getOrCreate()

df_route = spark.read.csv('routes.csv').toDF("Airline", "Ariline ID","Source Airport", "Source Airport ID", "Destination Airport", "Destination Airport ID", "Codeshare","Stops", "Equipment")
df_route = df_route.withColumn("Stops", df_route["Stops"].cast(IntegerType()))
df_route = df_route.withColumn("Destination airport ID", df_route["Destination airport ID"].cast(IntegerType()))

def main():
   functions.menu()
   option = int(input("\nEnter your choice.\n"))
   while option != 0:
      if option == 1:
         functions.mostNonStop()
      else:
         print("Invalid option.\n")
      
      functions.menu()
      option = int(input("Enter your choice.\n"))

   print("Thank you.\n")

        


if __name__ == "__main__":
    main()
