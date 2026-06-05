# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "245f8768-acdd-4c71-8fbe-6976fe1aa95c",
# META       "default_lakehouse_name": "healthcare1_msft_bronze",
# META       "default_lakehouse_workspace_id": "90911f80-867f-46bc-ae31-76eec7159d74"
# META     },
# META     "environment": {
# META       "environmentId": "894d3ef8-5976-4b7b-9ff5-8fac8e262664",
# META       "workspaceId": "90911f80-867f-46bc-ae31-76eec7159d74"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### WARNING
# The following notebook is intended to be read only. Please do not modify the contents of this notebook.


# CELL ********************

%run healthcare1_msft_config_notebook

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

%run healthcare1_msft_config_notebook {"enable_spark_setup" : true, "enable_packages_mount" : false}

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# PARAMETERS CELL ********************

inline_params = "{}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

from microsoft.fabric.hls.hds.services.file_orchestration_service import FileOrchestrationService
import json

# convert inline params into dictionary
inline_params_dict = json.loads(inline_params)

service = FileOrchestrationService(spark, 
                workspace_name=workspace_name,
                solution_name=solution_name,
                admin_lakehouse_name=administration_database_name,
                inline_params=inline_params_dict,
                one_lake_endpoint=one_lake_endpoint)

service.run()

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

mssparkutils.fs.unmount(packages_mount_name)

# METADATA ********************

# META {
# META   "frozen": false,
# META   "editable": false
# META }
