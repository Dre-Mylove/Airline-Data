from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
import functions # importing all functions from functions.py

# Create a spark session
spark = SparkSession.builder.appName('CS436_Project').getOrCreate()

# Loading file routes.csv and setting the column names as listed on https://openflights.org/data.html#schedule
df_route = spark.read.csv('routes.csv').toDF("Airline", "Ariline ID","Source Airport", "Source Airport ID", "Destination Airport", "Destination Airport ID", "Codeshare","Stops", "Equipment")
df_route = df_route.withColumn("Stops", df_route["Stops"].cast(IntegerType())) # Stops column by default was string so i cast it to int
df_route = df_route.withColumn("Destination airport ID", df_route["Destination airport ID"].cast(IntegerType())) # Destination airport ID column by default was string so i cast it to int

# Ended up not using this file but loaded columns as listed on https://openflights.org/data.html#schedule
#df_airlines = spark.read.csv('airlines.csv').toDF("Airline ID", "Name", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active")

# This is my driver function 
def main():
   functions.menu()                                # Displaying menu choices to make it easy to demonstrate functionality
   option = int(input("\nEnter your choice.\n"))
   while option != 0:
      if option == 1:
         functions.leastTraveled()
      elif option == 2:
         functions.mostTraveled()
      elif option == 3:
         functions.destinationMostTraveledTo()
      elif option == 4:
         functions.destinationLeastTraveledTo()
      else:
         print("Invalid option.\n")
      
      functions.menu()                             # Menu displays again so user can choose another option or exit
      option = int(input("Enter your choice.\n"))

   print("Thank you.\n")

if __name__ == "__main__":
    main()
