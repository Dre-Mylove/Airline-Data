from main import *
import pyspark
import folium as fol

def leastTraveled():
    print("Least traveled originating aiports: ")
    temp_int = int(input("\n How many would you like to see? \n"))  # lets user request the amount of columns shown
    df_route2 = df_route.dropna(subset="Source Airport")            # drops null values appearing in column so they dont appear in data
    temp_df = df_route2.groupby("Source Airport").count()
    temp_df.sort("count").show(temp_int)
    return

def mostTraveled():
    print("Most traveled aiports: ")
    temp_int = int(input("\n How many would you like to see? \n"))  # lets user request the amount of columns shown
    df_route2 = df_route.dropna(subset="Source Airport")            # drops null values appearing in column so they dont appear in data
    temp_df = df_route2.groupby("Source Airport").count()
    temp_df.sort(desc("count")).show(temp_int)
    return

def destinationMostTraveledTo():
    print("Airport most traveled to: ")
    temp_int = int(input("\n How many would you like to see? \n"))  # lets user request the amount of columns shown
    df_route2 = df_route.dropna(subset="Destination Airport")       # drops null values appearing in column so they dont appear in data
    temp_df = df_route2.groupby("Destination Airport").count()
    temp_df.sort(desc("count")).show(temp_int)
    return

def destinationLeastTraveledTo():
    print("Airport most traveled to: ")
    temp_int = int(input("\n How many would you like to see? \n"))  # lets user request the amount of columns shown
    df_route2 = df_route.dropna(subset="Destination Airport")       # drops null values appearing in column so they dont appear in data
    temp_df = df_route2.groupby("Destination Airport").count()
    temp_df.sort("count").show(temp_int)
    return

def menu():
    print("\n[1] Which is the least traveled originating airport? \n")
    print("\n[2] Which is the most traveled originating airport? \n")
    print("\n[3] Which airport is the most traveled to? \n")
    print("\n[4] Which airport is the least traveled to? \n")
    print("\n[0] Exit.\n")
    return