import folium as fol

m = fol.Map(location= [39.50010540342626, -119.7684657678575], zoom_start=14)

# Generate map
m.save('map.html')