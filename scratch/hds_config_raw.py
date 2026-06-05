# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
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


# PARAMETERS CELL ********************

# Workspace Config
workspace_name = '90911f80-867f-46bc-ae31-76eec7159d74' 
solution_name = 'd4354baf-f3c4-413a-ba36-0594cde9db03' 
one_lake_endpoint = "onelake.dfs.fabric.microsoft.com"

# Lakehouse/Database Config
administration_database_name = "13421d3a-8412-4217-a4e5-614ee74fe60b"

# HDS logs config
enable_hds_logs = True

# Misc Config
enable_packages_mount = False
enable_spark_setup = False

# Workload config
is_config_in_workload = True

packages_mount_name = "/hds_packages"
temp_packages_folder_path = f"tmp{packages_mount_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

import re
import os
import shutil
from notebookutils import mssparkutils
from sempy import fabric
from sempy.fabric import FabricRestClient
import pkg_resources

ENV_ID = "environmentId"

def extract_endpoint() -> str:
    """Helper function to resolve the OneLakeEndpoint at runtime."""
    endpoint = one_lake_endpoint

    all_paths = mssparkutils.fs.ls("/")

    if all_paths:
        path = all_paths[0].path
        match_endpoint = re.search(r'abfss://[^@]+@([^/]+)', path)    
    
        endpoint = match_endpoint.group(1) if match_endpoint else one_lake_endpoint
    return endpoint

def mount_and_copy_packages(source_path : str):
    try:
        mssparkutils.fs.unmount(packages_mount_name)
        mssparkutils.fs.mount(source_path, packages_mount_name)
        packages_mount_local_path = mssparkutils.fs.getMountPath(packages_mount_name)
        print(f"Successfully mounted {packages_mount_name}")

        # Copy the dm4h packages to a temp folder: `tmp/dm4h_packages`
        shutil.copytree(f"{packages_mount_local_path}", f"{temp_packages_folder_path}", dirs_exist_ok=True)

        print(f"Successfully copied the packages to: {temp_packages_folder_path}")
    except Exception as ex:
        print(f"An error occurred while mounting the source path: {ex}")
        raise

def is_guid(identifier: str) -> bool:
    """
    Validates if the identifier is a GUID.
    """
    guid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
    return bool(guid_pattern.match(identifier))
    
def is_valid_artifact_identifiers(workspace_identifier: str, artifact_identifier) -> bool:
    """
    Validates to ensure there is no mismatch in Identifiers. They should both be artifact GUIDs or Names.
    """
    return is_guid(identifier=workspace_identifier) == is_guid(identifier=artifact_identifier)

def check_fabric_runtime(expected_spark_version_regex=r"^3\.5.*"):
    """
    Checks the current Apache Spark version against a specified regex pattern to ensure compatibility with a specific Fabric runtime version, in this case, runtime 1.3.
    
    The function is designed to validate that the environment in which it is running aligns with the expected version requirements of Spark, as dictated by the regex pattern provided. This is crucial for applications that depend on specific features or behaviors available only in certain versions of Spark.
    
    Parameters:
    - expected_spark_version_regex (str): A regex pattern representing the expected version of Spark. The default pattern checks for version 3.4 and potentially any subversions following it. The pattern should start with the caret symbol (^) to ensure the version starts with the specified numbers, and can include wildcards or specific characters to match against the actual Spark version.
    
    Raises:
    - RuntimeError: If the current Spark version does not match the expected regex pattern. The error message includes the current Spark version, the expected version pattern (formatted for readability), and a link to documentation on how to switch Fabric runtimes.    
    """
    # Retrieve the current Apache Spark version
    spark_version = spark.version

    # Format the expected regex pattern for display purposes
    formatted_expected_version = expected_spark_version_regex[1:].replace(r"\.", ".")
    
    # Check if the current version matches the expected pattern
    if not re.match(expected_spark_version_regex, spark_version):
        # Raise a runtime error with the detailed instructions
        raise RuntimeError(
            f"Your current spark version ({spark_version}) does not match the required version pattern ({formatted_expected_version}).\n"
            "Please ensure you are using Fabric Runtime 1.3. \n"
            "For more information on switching runtimes, visit: "
            "https://learn.microsoft.com/en-US/fabric/data-engineering/runtime#multiple-runtimes-support"
        )

def check_envionment_publishing_state():
    """
    Checks the publishing state of the current environment as well as the installed packages. Throws an exception if conditions are not met.
    """
    runtime_context = mssparkutils.runtime.context
    environment_published = False
    
    if ENV_ID in runtime_context and len(runtime_context[ENV_ID]) > 0:
        
        environment_details = None
        environment_name = None
        try:
            workspace_id = runtime_context['currentWorkspaceId']
            environment_id = runtime_context[ENV_ID]
            environment_details = get_environment_details(workspace_id, environment_id)
            environment_name = environment_details["displayName"]
            publish_state = str(environment_details['properties']['publishDetails']['state']).lower()
        except Exception:
            print("Error occurred getting the current environment, checking installed packages")
        
        if environment_details:
            if publish_state == "running" or publish_state == "waiting":
                raise RuntimeError(f"The current environment ({environment_name}) publishing state is {publish_state}, please wait until publishing succeeds.")
            elif publish_state == "cancelling" or publish_state == "failed":
                raise RuntimeError(f"The current environment ({environment_name}) publishing state is {publish_state}, please fix the environment.")
            print(f"Notebook environment ({environment_name}) was published successfully")
        environment_published = True
        
    missing_required_packages = get_missing_required_packages()

    if missing_required_packages:
        if environment_published:
            raise RuntimeError("Environment was published, but packages are not installed: " + str(missing_required_packages) +  ". Try restarting the spark session.")
        raise RuntimeError("Environment not found and required packages are not installed: " + str(missing_required_packages))

def get_environment_details(workspace_id, environment_id):
    client = FabricRestClient()
    response = client.get(f"v1/workspaces/{workspace_id}/environments/{environment_id}")
    response_json = response.json()
    return response_json

def get_missing_required_packages():

    required_packages = ['hds', 'dtt']
    installed_packages = {pkg.key: pkg.version for pkg in pkg_resources.working_set}

    missing_packages = [pkg for pkg in required_packages if pkg not in installed_packages]
    return missing_packages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Check the current environment state
check_envionment_publishing_state()

# Validate the current Fabric runtime version
check_fabric_runtime()

# Construct absolute paths
one_lake_endpoint = extract_endpoint()

if is_valid_artifact_identifiers(workspace_name, solution_name):
    # Resolves correct path if the config files are in a lakehouse
    if not is_config_in_workload:
        solution_name = f"{solution_name}/Files"

    data_manager_solution_path = f"abfss://{workspace_name}@{one_lake_endpoint}/{solution_name}"
    data_manager_config_path = f"{data_manager_solution_path}/DMHConfiguration"
    data_manager_packages_path = f"{data_manager_config_path}/_internal/packages"
    data_manager_sample_data_path = f"{data_manager_solution_path}/DMHSampleData"
else:
    raise ValueError(f"Mismatched artifact identifiers:  workspace_name ({workspace_name}) and solution_name ({solution_name}) should both be either Artifact `GUIDs` or `names`.")

# Mount the datamanager config path
if enable_packages_mount:
    mount_and_copy_packages(source_path=data_manager_packages_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import SparkSession

if enable_spark_setup:
    spark = SparkSession.builder \
        .appName("HDS") \
        .config("spark.sql.caseSensitive", "False") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "True") \
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "True") \
        .config("spark.sql.parquet.native.writer.memory", "268435456") \
        .config("spark.hds.enable_hds_logs", str(enable_hds_logs)) \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()

    print("Spark configuration setup completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }
