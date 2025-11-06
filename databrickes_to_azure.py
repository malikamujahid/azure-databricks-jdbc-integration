# Databricks notebook source
jdbcHostname = "mymvp.database.windows.net"   # Your SQL Server name
jdbcPort = 1433
jdbcDatabase = "#########"                      # Your database name
jdbcUsername = "#########"              # The full SQL admin username
jdbcPassword = "#########"             # Replace with your real password

# Build JDBC URL
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
df = (spark.read
.format("jdbc")
.options(url= jdbcUrl,
dbtable= "[SalesLT].[Customer]",
user= jdbcUsername,
password= jdbcPassword
)
.load())
display(df) 

# COMMAND ----------

jdbcHostname = "mymvp.database.windows.net"   # Your SQL Server name
jdbcPort = 1433
jdbcDatabase = "#########"                      # Your database name
jdbcUsername = "#########"              # The full SQL admin username
jdbcPassword = "#########"             # Replace with your real password

# Build JDBC URL
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

df=[("Pakistan", "Islamabad"), ("India", "New Delhi"), ("China", "Beijing")]
columns= ["Country", "Capital"]
test_df= spark.createDataFrame(df, columns)

(test_df.write
 .format("jdbc")
 .mode("overwrite")
 .options(url= jdbcUrl,
 dbtable= "Country",
 user= jdbcUsername,
 password= jdbcPassword)
.save()
)


# COMMAND ----------

query= """
select * from [dbo].[Country]"""

jdbcHostname = "mymvp.database.windows.net"   # Your SQL Server name
jdbcPort = 1433
jdbcDatabase = "#########"                      # Your database name
jdbcUsername = "#########"              # The full SQL admin username
jdbcPassword = "#########"             # Replace with your real password

# Build JDBC URL
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
df= (spark.read
    .format("jdbc")
    .options(
        url= jdbcUrl,
            query= query,
            user= jdbcUsername,
            password= jdbcPassword
    )
    .load())

display(df)



