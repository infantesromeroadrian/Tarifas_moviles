 #1.1 INDICE - CONTENIDO
#%%
print('1. Introduction')
print('2. Importacion de librerias')
print('3. Lectura de datos')
print('4. Anilisis por columna')
print('5. Busqueda y analisis de datos nulos')
print('5. Reemplazamiento de datos nulos y atipicos')
print('6. Creacion de nuevas columnas')
print('7. Visualizacion de datos')
print('8. Analisis mediante pivot_table')
print('9. Creacion de nuevo dataset(probability)')
print('10. Conclusiones')
#%% md
# 1.2 CONTEXTO - PROYECTO
#%% md
#Trabajas como analista para el operador de telecomunicaciones Megaline. La empresa ofrece a sus clientes dos tarifas de prepago, Surf y Ultimate. El departamento comercial quiere saber cuál de los planes genera más ingresos
#para ajustar el presupuesto de publicidad.
#Vas a realizar un análisis preliminar de las tarifas basado en una selección de clientes relativamente pequeña. Tendrás los datos de 500 clientes de Megaline: quiénes son los clientes, de dónde son, qué tarifa usan y la
#cantidad de llamadas que hicieron y los mensajes de texto que enviaron en 2018. Tu trabajo es analizar el comportamiento de los clientes y determinar qué tarifa de prepago genera más ingresos.

#%% md
# 1.3 CARGA DE LIBRERIAS
#%%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import random as np
import scipy as np
#%% md
# 1.4 CARGA DE DATOS
#%%
calls_data = pd.read_csv('/Users/adrianinfantesromero/Desktop/AIR/Work/GitHub/Practicum/Tarifas_moviles/megaline_calls.csv')
internet_data = pd.read_csv('/Users/adrianinfantesromero/Desktop/AIR/Work/GitHub/Practicum/Tarifas_moviles/megaline_internet.csv')
messages_data = pd.read_csv('/Users/adrianinfantesromero/Desktop/AIR/Work/GitHub/Practicum/Tarifas_moviles/megaline_messages.csv')
plans_data = pd.read_csv('/Users/adrianinfantesromero/Desktop/AIR/Work/GitHub/Practicum/Tarifas_moviles/megaline_plans.csv')
users_data = pd.read_csv('/Users/adrianinfantesromero/Desktop/AIR/Work/GitHub/Practicum/Tarifas_moviles/megaline_users.csv')
#%%
calls_data
#%%
internet_data
#%%
messages_data
#%%
plans_data
#%%
users_data
#%% md
# 2.0 ANALISIS DE DATOS & NULOS
#%% md
## 2.1 MESSAGES
#%%
messages_data.info()
# podemos observar que la composicion del dataset donde vemos el tipo de dato que contine cada columna.
#%%
messages_data.head()
#%%
messages_data.isnull().sum()
#%% md
## 2.2 USERS DATA
#%%
users_data.info()
#%%
users_data.isnull().sum()
#%% md
## 2.3 CALLS DATA
#%%
calls_data.info()
#%%
calls_data.isnull().sum()
#%% md
## 2.4 INTERNET DATA
#%%
internet_data.info()
#%%
internet_data.isnull().sum()
#%% md
## 2.5 PLANS DATA
#%%
plans_data.info()
#%%
plans_data.isnull().sum()
#%% md
#Vemos que hay varios datos nulos en el dataset users_data pero observamos que basicamente son todos aquellos que no tienen aun fecha de baja de la tarifa. Por lo que realmente esos datos nulos igualmente si nos aportan informacion. Por lo que vamos a cambiarlos por datos numericos para que nos puedan servir en el analisis futuro.
#%% md
#  3. PREPRACION DE DATASETS
#%% md
## 3.1. MESSAGES
#%%
messages_data['message_date'] = pd.to_datetime(messages_data['message_date'], format='%Y-%m-%d')
#%%
messages_data['month'] = messages_data['message_date'].dt.month
#%%
messages_data['day_of_week'] = pd.to_datetime(messages_data['message_date']).dt.day_name()
#%%
pd.pivot_table(messages_data, index = ['user_id', 'month'], values = ['id'], aggfunc = 'count')
#%%
messages_data
#%% md
## 3.2. CALLS
#%%
calls_data['call_date'] = pd.to_datetime(calls_data['call_date'], format='%Y-%m-%d')
#%%
calls_data['month'] = calls_data['call_date'].dt.month
#%%
calls_data['duration'] = calls_data['duration'].apply(np.ceil)
#%%
calls_data['day_of_week'] = pd.to_datetime(calls_data['call_date']).dt.day_name()
#%%
pd.pivot_table(calls_data, index = ['user_id', 'month'], values = ['duration'], aggfunc = 'sum')
#%%
calls_data
#%% md
## 3.3 INTERNET
#%%
internet_data['session_date'] = pd.to_datetime(internet_data['session_date'], format='%Y-%m-%d')
internet_data['month'] = internet_data['session_date'].dt.month
#%%
internet_data['gb_used'] = internet_data['mb_used'] / 1024 # esto es para pasar de mb a gb
#%%
internet_data['gb_used'] = internet_data['gb_used'].apply(np.ceil)
#%%
internet_data['day_of_week'] = pd.to_datetime(internet_data['session_date']).dt.day_name()
#%%
pd.pivot_table(internet_data, index = ['user_id', 'month'], values = ['gb_used'], aggfunc = 'count')
#%%
internet_data
#%% md
## 3.3 MERGE DE TABLAS
#%% md
## 3.3.1 TABLA DE CONSUMO
#%%
consume_table = pd.merge(pd.merge(pd.pivot_table(calls_data, index = ['user_id', 'month'], values = ['duration'], aggfunc = 'sum'), pd.pivot_table(messages_data, index = ['user_id', 'month'], values = ['id'], aggfunc = 'count'), on = ['user_id', 'month'], how = 'outer'), pd.pivot_table(internet_data, index = ['user_id', 'month'], values = ['gb_used'], aggfunc = 'sum'), on = ['user_id', 'month'], how = 'outer').reset_index()
#%%
consume_table
#%% md
## 3.2.2 TABLA DE USUARIO & PLAN
#%%
users_plans = pd.merge(users_data, plans_data, left_on = 'plan', right_on = 'plan_name', how = 'left').drop('plan_name', axis = 1)
#%%
users_plans
#%% md
## 3.2.3 TABLA DE CONSUMO DEL USUARIO
#%%
users_plans_consume = pd.merge(users_plans, consume_table, on = 'user_id', how = 'left')
#%%
users_plans_consume
#%% md
# 4.0 EDA - 1os ANALISIS DE DATOS SIMPLES
#%% md
## 4.1 CATEGORICOS
#%%
users_plans_consume['plan'].value_counts()
#%%
users_plans_consume['plan'].value_counts().plot(kind = 'pie', autopct = '%1.1f%%')
#%% md
#Primeros insghts:
#- El 68,6 de los usuarios eligen plan surf.
#- El restante '31,4% eligen el plan ultimate
#%%
users_plans_consume['city'].value_counts()
#%%
users_plans_consume['city'].value_counts().plot(kind = 'bar', figsize = (20, 5))
#%% md
#Observamos:
#- La ciudad donde mas clientes tiene nuestra empresa es New York con una clara diferencia del resto.
##%% md

#%%
users_plans_consume.groupby('plan')['city'].value_counts()
#%%
users_plans_consume[users_plans_consume['plan'] == 'surf']['city'].value_counts().plot(kind = 'bar', figsize = (20, 5))
#%% md
#observamos:
#- La ciudad de New York es la que recoge mayor cantidad del plan surf.
#%%
users_plans_consume[users_plans_consume['plan'] == 'ultimate']['city'].value_counts().plot(kind = 'bar', figsize = (20, 5))
#%% md
#Observamos:
#- Pese a que New York es la que recoge de nuevo mas usuarios la distribución por ciudad del plan ultimate es mayor ya que recoge mas ciudades con una cantidad de usuarios notable.
#- Las ciudades con mayor cantidad de usuarios estan en New York, Los Angeles, Chicago, Miami, etc...
#- Esto nos puede indicar que pese a que New York es la que recoge mas usuarios puede ser que la distribución economica de la ciudad no sea tan grande ya que las tarifas mas baratas estan en esa ciudad mientras que en el resto de ciudades si tienen la economica suficiente para obtener la tarifa ultimate.

#Estas conclusiones se deberian enviar al equipo de ventas y marketing de manera que a la hora de invertir en promos estos datos sean kPIs de entrada en sus filtros.
#%% md
## 4.2 CUANTITATIVOS
#%%
users_plans_consume['age'].value_counts()
#%%
users_plans_consume['age'].value_counts().plot(kind = 'hist', bins = 58)
#%% md
#observamos:
#- Los mayores picos de edad del usurario estan entre los 20 y 30, siendo la edad entre los 30 y 40 donde hay mas usuarios, se mantiene estbale hasta los 50 y apartir de los 60 los clientes son mucho mas escasos.
#- Tenemos detalles como que hay usuarios de 9, 11, 12 y 13 años. Lo que indica que son menores. Puede ser que tengan autorizacion paterna o que el dato sea erroneo.
#%%
users_plans_consume[users_plans_consume['plan'] == 'surf']['age'].value_counts()
#%%
users_plans_consume[users_plans_consume['plan'] == 'surf']['age'].value_counts().plot(kind = 'hist', bins = 58)
#%% md
#Al igual que en la tabla anterior podemos observar que la mayoria del publico se encuentra entre las edades de 20 y 35 años pero observamos un reparto mayor entre las edades inferiores a 20 cosa que en el anterior grafico no osbservabamos.
#%%
users_plans_consume[users_plans_consume['plan'] == 'ultimate']['age'].value_counts()
#%%
users_plans_consume[users_plans_consume['plan'] == 'ultimate']['age'].value_counts().plot(kind = 'hist', bins = 58)
#%% md
#Observamos:
#- Este datos si que no nos lo esperabamos. La mayoria de usuarios tienen una edad comprendida entre 4 a 20 años
#- La edad de 13-14 años el mayor pico de venta de dicha tarifa.
#%%
users_plans_consume['churn_date'] = users_plans_consume['churn_date'].str[5:7]
#%%
users_plans_consume['churn_date'].value_counts()
#%%
users_plans_consume['churn_date'].hist()
#%% md
#Claramente podemos observar que los meses del año con mas bajas son los meses de el total de usuarios son los meses de Noviembre, Diciembre, Septiembre y Octubre.
#%%
sns.countplot(x = 'churn_date', data = users_plans_consume[users_plans_consume['plan'] == 'surf'])
#%% md
#En este grafico apreciamos las bajas de los usuarios del plan surf donde podemos observar:
#- Que la mayoria de bajs ocurren en los ultimos 4 meses del año. Esto puede ser que arroje la siguiente información:
#- Los usuarios descontentos al haber probado el plan durante el año deciden cambiar de empresa.
#- La competencia durante los ultimos meses del año sacan nuevas ofertas y es por eso que ocurre la migracion de los usuarios.

#%%
sns.countplot(x = 'churn_date', data = users_plans_consume[users_plans_consume['plan'] == 'ultimate'])
#%% md
#Al observar la grafica de abandono de los usuarios del plan Ultimate nos damos cuenta de una coincidencia o similitud entre los dos graficos.
#- Ambos grupos de usuarios tienen la mayor cantidad de abandonos durante el mes de noviembre. Esto deja claro que la competencia debe estar lanzando nuevos productos que los usuarios no pueden dejar pasar por alto.
#- Al igual que en el grafico anterior los abandonos se centran en el ultimo cuarto del año.

#Como conclusiones deberiamos de intentar reforzar la satisfaccion y fidelizacion de los usuarios en el ultimo cuarto del año.
##%%

#%% md
# 5.0 ANALISIS DE DATOS COMPLEJOS.
#%% md
## 5.1 ANALISIS DE CONSUMO
#%%
users_plans_consume.groupby('plan')['duration', 'id', 'gb_used'].mean()
#%% md
#La media de los usuarios de cada plan es muy similar, por lo que no creo que sea un factor determinante para el modelo de predicción
#%%
users_plans_consume.groupby('plan')['duration', 'id', 'gb_used'].median()
#%%
users_plans_consume.corr()
#%% md
#PRIMEROS INSIGHTS
#%% md
#- Los resultados cercanos a 1 nos indican que hay una correlación positiva entre las variables, mientras que los cercanos a -1 nos indican que hay una correlación negativa entre las variables.
#- La mediana tambien es muy similar, lo que quiere decir que la distribución de los datos es muy similar en ambos planes. Aparte de la media y la mediana que no nos estan dando información.
#- La media de los usuarios de cada plan es muy similar, por lo que no creo que sea un factor determinante para el modelo de predicción.

#No vemos una correlación de gasto directa en base a el tipo de plan del usuario o eso pensariamos a priori pero estamos examinando los datos en el conjunto del año de manera que vamos a realizar un examen a lo largo del año.
#%% md
## 5.2 - EVOLUCION DE CONSUMO DE MINUTOS DE LLAMADAS DURANTE EL AÑO
#%%
sns.lineplot(data = users_plans_consume, x = 'month', y = 'duration', hue = 'plan')
#%% md
#En este primer analisis observamos que si que hay una relacion muy estrecha entre el uso de los minutos de llamadas de ambos tipos de usuarios. Pero vemos 3 apreciaciones importantes:
#- En el 2o mes vemos una diferenciacion de los usuarios de cada plan siendo los usuarios del plan ultimate los que usan mas minutos de llamadas en ese mes.
#- En el grafico podemos observar como lo usuarios de ambos planes tienen una tendencia positiva en el uso de los minutos de llamadas a lo largo del año. Es decir todos los meses gastan mas. Eso lo podriamos traducir como una satisfaccion por el plan por parte del usuario.
#- La varianza de los usuarios de cada plan pese a que en los 2 primeros meses si es notoria como en el 4o mes se comienza a unificar el uso de los minutos de los usuarios de cada plan.
#- Por ultimo vemos algo que a la larga puede provocar que el usuario cancele su servicio por no ver amortizado el costo de su servicio ya que observamos que los usuarios del plan surf estab consumiendo mas minutos de llamadas apartir del mes 3 hasta el mes 7 pese no llegar a su limite de minutos. Mientras que por contraste los usuarios del plan ultimate desde el mes 3 usan menos minutos de llamadas que los usuarios del plan surf pese a que su limite es mucho mas alto.
#- En general podemos llegar a las conclusiones de que el usuario del plan ultimate a obtenido dicho plan por algo diferenta a los minutos de llamadas o que simplemente no esta bien informado y eso puede provocar su ida del plan.
#%%
calls_data.groupby(['user_id', 'month'])['duration'].agg(['count', 'sum']).reset_index()
#%% md
#Esta variable la tenemos que analizar con un extra de atencion ya que la cantidad de minutos nos da bastante info pero aparte tenemos que tener en cuenta la duracion de cada llamada para observar algun comportamiento adicional en nuestros usuarios.
#%%
calls_data[calls_data['user_id'].isin(users_plans_consume[users_plans_consume['plan'] == 'surf']['user_id'])].groupby('user_id')['duration'].mean().mean()
#%%
calls_data[calls_data['user_id'].isin(users_plans_consume[users_plans_consume['plan'] == 'ultimate']['user_id'])].groupby('user_id')['duration'].mean().mean()
#%% md
#- Observamos que el numero de llamadas es muy similar entre cada plan.
#%% md
## 5.3 - EVOLUCION DE CONSUMO DE MENSAJES DURANTE EL AÑO
#%%
sns.lineplot(data = users_plans_consume, x = 'month', y = 'id', hue = 'plan')
#%% md
#En este segundo analisis observamos:
#- Una evolucion positiva a lo largo del año en el uso de SMS por ambos usuarios de cada plan
#- Observamos una varianza mayor en el uso de SMS por parte de los usuarios del plan ultimate en comparacion con los usuarios de el plan surf.
#- El uso es proporcionalmente mayor de cuenta de los usurarios del plan ultimate en comparacion con los usuarios del plan surf. Pero es logico ya que su limite es mayor.
#- En el plan ultimate hay 2 picos de uso en el mes 3 y 5 puede ser que en dicho pais en esas fechas sean festivos y por ello el mayor uso de SMS en esas fechas y ademas en esas fechas si vemos una varianza notable.
#- Por ultimo apartir del mes 8 vemos una unificacion de la varianza de cada plan e incluso aproximamiento en el uso de SMS por ambos usuarios de cada plan.
#%% md
## 5.4 - EVOLUCION DE CONSUMO DE GB DURANTE EL AÑO
#%%
sns.lineplot(data = users_plans_consume, x = 'month', y = 'gb_used', hue = 'plan')
#%% md
#En este ultimo analisis observamos:
#- Como hay un incremento notorio del uso de gb del 1er al 2o mes proporcional a cada plan y como se va normalizando hasta final de año.
#- Como en el resto de analisis de los keyItems observamos una varianza mayor en los primeros 2 meses de ambos planes hasta llegar practicamente a unificarse a final de año.
#- Otros de los datos que son preocupantes es que ambos usuarios de cada plan desde la 2o mitad del año usan la misma cantidad de datos pese a que los del plan ultimate pagan mas que los del plan surf. Esto puede provocar mas bajas a los largo del año que analizaremos mas adelante.
#- Por ultimo no vemos un crecimiento en el uso en comparacion de los otros graficos.
#%% md
# 6.0 PRUEBA DE HIPOTESIS
#%% md
# 6.1 Primera Hipotesis.
#%% md
## 6.1 HAY UNA DIFERENCIA DE INGRESO PROMEDIO USUARIOS ULTIMATE Y SURF.
#%% md
#La hipotesis nula es:

#H0: el ingreso promedio de los usuarios de surf es igual al de los usuarios de ultimate en el dataset users_plans_consume

#La hipotesis alternativa es:

#H1: el ingreso promedio de los usuarios de surf es diferente al de los usuarios de ultimate en el dataset users_plans_consume
#%% md
#Para comprorbar la hipotesis vamos a hacer un test de hipotesis de dos colas con un nivel de significacion del 5% (alpha = 0.05)
#Ello lo hacemos con la funcion ttest_ind de la libreria scipy.stats
#%%
users_plans_consume['income'] = users_plans_consume['duration'] * 0.03 + users_plans_consume['id'] * 0.03 + users_plans_consume['gb_used'] * 10
#%%
users_plans_consume
#%% md
#El income es el total de los minutos, mensajes y gb consumidos por el usuario multiplicado por el precio de cada uno de ellos
#%%
users_plans_consume.groupby('plan')['income'].mean()
#%% md
#El ingreso promedio de los usuarios de surf es 417 y el de los usuarios de ultimate es 426. Es decir un usuario de surf gasta mas que un usuario de ultimate.
#%%
from scipy import stats
#%%
stats.ttest_ind(users_plans_consume[users_plans_consume['plan'] == 'surf']['income'], users_plans_consume[users_plans_consume['plan'] == 'ultimate']['income'], equal_var = False)
#Lo que hace esta funcion es calcular el estadistico t y el p-value de la hipotesis nula de que las medias de los dos grupos son iguales
#%% md
#Esto quiere decir que podemos rechazar la hipotesis nula de que las medias de los dos grupos son iguales ya que el p-value es mayor que 0.05 y eso aparece como nan porque el p-value es muy pequeño.
#%% md
## 6.2 HAY UNA DIFERENCIA ENTRE LOS INGRESOS PROMEDIOS DE LOS USUARIOS DE NEW YORK CON EL RESTO DE USUARIOS.
#%%
users_plans_consume[users_plans_consume['city'] == 'New York-Newark-Jersey City, NY-NJ-PA MSA']['income'].mean()
#%%
users_plans_consume[users_plans_consume['city'] != 'New York-Newark-Jersey City, NY-NJ-PA MSA']['income'].mean()
#%% md
#La hipotesis nula es:
#H0: el ingreso promedio de los usuarios de la ciudad de nueva york no es igual al de los usuarios de otras ciudades en el dataset users_plans_consume


#La hipotesis alternativa es:
#H1: el ingreso promedio de los usuarios de la ciudad de nueva york es igual al de los usuarios de otras ciudades en el dataset users_plans_consume

#%%
stats.ttest_ind(users_plans_consume[users_plans_consume['city'] == 'New York-Newark-Jersey City, NY-NJ-PA MSA']['income'], users_plans_consume[users_plans_consume['city'] != 'New York-Newark-Jersey City, NY-NJ-PA MSA']['income'], equal_var = False) # lo que hace esta funcion es calcular el estadistico t y el p-value de la hipotesis nula de que las medias de los dos grupos son iguales
#%% md
#Esto quiere decir que podemos rechazar la hipotesis nula de que las medias de los dos grupos difieren ya que el p-value es mayor que 0.05 y eso aparece como nan porque el p-value es muy pequeño
