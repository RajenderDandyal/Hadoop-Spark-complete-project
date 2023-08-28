import os

### Set Environment Variables
os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'postgres'
os.environ['password'] = 'root'

### Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

### Set Other Variables
appName="USA Prescriber Research Report"
current_path =  os.getcwd()
print(current_path)
# staging_dim_city = current_path +'/staging/dimension_city'
staging_dim_city = "PrescPipeline/staging/dimension_city"
# staging_fact = current_path + '/staging/fact'
staging_fact = "PrescPipeline/staging/fact"

output_city = "PrescPipeline/output/dimension_city"
output_fact = "PrescPipeline/output/fact"









