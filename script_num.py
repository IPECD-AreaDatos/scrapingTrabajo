#Transformacion de totales
df['total'] = df['total'].str.replace(",","")
df['total'] = df['total'].astype(int)


serie = df['total'] #-->Trabajamos con una serie de datos
print(serie)

