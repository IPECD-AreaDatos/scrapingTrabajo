#Transformacion de totales
df['total'] = df['total'].str.replace(",","")
df['total'] = df['total'].astype(int)

