from main import df_route
import folium as fol

def mostNonStop():
    print("Airline with most nonstop flights is ")
    df_route.dropna()
    df_route.groupby("Source Airport").sum("Stops").show(2)
    return

def menu():
    print("\n[1] Which airport offers the most non-stop flights.\n")
    print("[0] Exit.\n")