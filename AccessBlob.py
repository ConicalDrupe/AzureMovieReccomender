# From Documentation https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage

service_credential = dbutils.secrets.get(scope="MovieScope2",key="clientsecret")

spark.conf.set("fs.azure.account.auth.type.sgmovies01222023.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sgmovies01222023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sgmovies01222023.dfs.core.windows.net", "clientid1")
spark.conf.set("fs.azure.account.oauth2.client.secret.sgmovies01222023.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sgmovies01222023.dfs.core.windows.net", "https://login.microsoftonline.com/tenantid/oauth2/token")

#Testing
path = "wasbs://validated@sgmovies01222023.blob.core.windows.net/Data/movies.csv"
df = spark.read.format("csv").option("header", "true").load(path)

display(df)
