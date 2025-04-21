import os
import threading
import traceback
import xml.etree.ElementTree as ET
from xml.dom import minidom
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timedelta

@dataclass
class TaskInfo:
    PackageName: Optional[str] = None
    PackagePath: Optional[str] = None
    EventHandlerName: Optional[str] = None
    EventHandlerType: Optional[str] = None
    EventType: Optional[str] = None
    TaskName: Optional[str] = None
    TaskType: Optional[str] = None
    TaskSqlQuery: Optional[str] = None
    ContainerName: Optional[str] = None
    ContainerType: Optional[str] = None
    ContainerExpression: Optional[str] = None
    ContainerEnum: Optional[str] = None
    Variables: Optional[str] = None
    Parameters: Optional[str] = None
    Expressions: Optional[str] = None
    ExecuteProcessDetails: Optional[str] = None
    FileSystemSourcePath: Optional[str] = None
    FileSystemDestinationPath: Optional[str] = None
    SourceComponent: Optional[str] = None
    TargetComponent: Optional[str] = None
    SourceType: Optional[str] = None
    TargetType: Optional[str] = None
    TargetTable: Optional[str] = None
    SendMailTask: Optional[str] = None
    ScriptTask: Optional[str] = None
    FTPTask: Optional[str] = None
    ExecutePackage: Optional[str] = None
    ResultSetDetails: Optional[str] = None
    SeqTaskName: Optional[str] = None
    ForeachTaskName: Optional[str] = None
    ForloopTaskName: Optional[str] = None
    ConnectionName: Optional[str] = None
    SourceConnectionName: Optional[str] = None
    TargetConnectionName: Optional[str] = None
    TaskComponentDetails: Optional[str] = None

@dataclass
class ConnectionInfo:
    ConnectionName: Optional[str] = None
    ConnectionType: Optional[str] = None
    ConnectionString: Optional[str] = None
    ConnectionExpressions: Optional[str] = None
    ConnectionID: Optional[str] = None
    IsProjectConnection: Optional[str] = None

@dataclass
class ContainerInfo:
    ContainerName: Optional[str] = None
    ContainerType: Optional[str] = None
    ContainerExpression: Optional[str] = None

@dataclass
class VariableInfo:
    Name: Optional[str] = None
    Value: Optional[str] = None
    DataType: Optional[str] = None
    Namespace: Optional[str] = None
    IsParameter: int = 0

@dataclass
class TaskParameterInfo:
    ParameterName: Optional[str] = None
    ParameterType: Optional[str] = None
    DataType: Optional[str] = None
    Value: Optional[str] = None
    DtsVariableName: Optional[str] = None

@dataclass
class DataFlowTaskInfo:
    ColumnName: Optional[str] = None
    ColumnType: Optional[str] = None
    DataType: Optional[str] = None
    TargetColumn: Optional[str] = None
    componentName: Optional[str] = None
    DataConversion: Optional[str] = None
    PackageName: Optional[str] = None
    PackagePath: Optional[str] = None
    TaskName: Optional[str] = None
    isEventHandler: Optional[str] = None
    componentPropertyDetails: Optional[str] = None
    ColumnPropertyDetails: Optional[str] = None

@dataclass
class PrecedenceConstraintInfo:
    PrecedenceConstraintFrom: Optional[str] = None
    PrecedenceConstraintTo: Optional[str] = None
    PrecedenceConstraintValue: Optional[str] = None
    PrecedenceConstraintLogicalAnd: Optional[str] = None
    PrecedenceConstraintEvalOP: Optional[str] = None
    PrecedenceConstraintExpression: Optional[str] = None
    ContainerName: Optional[str] = None
    PackageName: Optional[str] = None
    PackagePath: Optional[str] = None

@dataclass
class ProjectParameterInfo:
    ParameterName: Optional[str] = None
    DataType: Optional[str] = None
    Value: Optional[str] = None

@dataclass
class PackageAnalysisResult:
    PackageName: Optional[str] = None
    CreatedDate: Optional[datetime] = None
    CreatedBy: Optional[str] = None
    Tasks: List[TaskInfo] = field(default_factory=list)
    Seqtasks: List[TaskInfo] = field(default_factory=list)
    Foreachtasks: List[TaskInfo] = field(default_factory=list)
    Forlooptasks: List[TaskInfo] = field(default_factory=list)
    Connections: List[ConnectionInfo] = field(default_factory=list)
    ExecutionTime: Optional[timedelta] = None
    PackagePath: Optional[str] = None
    Containers: List[ContainerInfo] = field(default_factory=list)
    DTSXXML: Optional[str] = None
    SequenceContainerTaskCount: List[TaskInfo] = field(default_factory=list)
    ForeachContainerTaskCount: List[TaskInfo] = field(default_factory=list)
    ForLoopContainerTaskCount: List[TaskInfo] = field(default_factory=list)
    Variables: List[VariableInfo] = field(default_factory=list)
    DataFlowTaskDetails: List[DataFlowTaskInfo] = field(default_factory=list)
    PrecedenceConstraintDetails: List[PrecedenceConstraintInfo] = field(default_factory=list)
    ExtractTaskDetails: List[TaskInfo] = field(default_factory=list)
    ProjectParameterDetails: List[ProjectParameterInfo] = field(default_factory=list)

class SSISPackageAnalyzer:
    def __init__(self, package_folder, metadata_connection_string, package_analysis_file_path, dataflow_file_path, package_details_file_path, data_save_type):
        self.container_count = 0
        self.container_task_count = 0
        self._connection_string = metadata_connection_string
        self._package_folder = package_folder
        self.processed_package_paths = set()
        self.PackagePath = ""
        self.PackageName = ""
        self.ComponentCount = 0
        self.PackageAnalysisFilePath = package_analysis_file_path
        self.DataFlowlFilePath = dataflow_file_path
        self.PackageDetailsFilePath = package_details_file_path
        self.DataSaveType = data_save_type
        self.ComponentNameCheck = []
        self.variables_metadata = []

    def analyze_all_packages(self):
        self.truncate_table()
        directories = [
            os.path.join(dp, f) for dp, dn, _ in os.walk(self._package_folder) 
            for f in dn
        ]
        for directory in directories:
            if "\\obj\\" in directory.lower():
                continue
            try:
                package_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".dtsx")]
                connection_manager_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".conmgr")]
                param_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".params")]

                for package_path in package_files:
                    if package_path in self.processed_package_paths:
                        continue
                    try:
                        self.processed_package_paths.add(package_path)
                        self.analyze_single_package(package_path)
                    except Exception as ex:
                        self.log_error(package_path, ex)

                for connection_manager_path in connection_manager_files:
                    if connection_manager_path in self.processed_package_paths:
                        continue
                    try:
                        self.processed_package_paths.add(connection_manager_path)
                        self.analyze_single_connection_manager(connection_manager_path)
                    except Exception as ex:
                        self.log_error(connection_manager_path, ex)

                for param_file in param_files:
                    if param_file in self.processed_package_paths:
                        continue
                    try:
                        self.processed_package_paths.add(param_file)
                        self.analyze_param_manager(param_file)
                    except Exception as ex:
                        self.log_error(param_file, ex)
            except Exception as ex:
                print(f"Error accessing directory {directory}: {str(ex)}")
        self.save_variable_metadata(self.PackageDetailsFilePath)
        print("Completed...")

    def truncate_table(self):
        pass

    def analyze_single_package(self, package_path):
        tree = ET.parse(package_path)
        root = tree.getroot()
        namespace = {'DTS': 'www.microsoft.com/SqlServer/Dts'}

        package_name = os.path.basename(package_path)
        package_folder = os.path.dirname(package_path)

        self.extract_variables(root, package_name, package_folder, namespace)

    def extract_variables(self, root, package_name, package_folder, ns):
        for variable in root.findall(".//DTS:Variable", ns):
            name = variable.get('{www.microsoft.com/SqlServer/Dts}ObjectName')
            data_type = variable.get('{www.microsoft.com/SqlServer/Dts}DataType')
            namespace_scope = variable.get('{www.microsoft.com/SqlServer/Dts}Namespace')

            value_node = variable.find("DTS:Value", ns)
            value = value_node.text if value_node is not None else ""

            self.variables_metadata.append({
                'VariableName': name,
                'DataType': data_type,
                'Namespace': namespace_scope,
                'Value': value,
                'PackageName': package_name,
                'PackagePath': package_folder
            })

    def save_variable_metadata(self, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            df = pd.DataFrame(self.variables_metadata)
            excel_path = os.path.join(file_path if os.path.isdir(file_path) else os.path.dirname(file_path), "Variables.xlsx")
            df.to_excel(excel_path, index=False)
            print(f"Saved variable metadata to {excel_path}")
        elif self.DataSaveType.upper() == "SQL":
            import pyodbc
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()

            cursor.execute("""
                IF OBJECT_ID('dbo.SSISVariables', 'U') IS NOT NULL DROP TABLE dbo.SSISVariables;
                CREATE TABLE dbo.SSISVariables (
                    VariableName NVARCHAR(255),
                    DataType NVARCHAR(50),
                    Namespace NVARCHAR(255),
                    Value NVARCHAR(MAX),
                    PackageName NVARCHAR(255),
                    PackagePath NVARCHAR(500)
                )
            """)
            for row in self.variables_metadata:
                cursor.execute("""
                    INSERT INTO dbo.SSISVariables (VariableName, DataType, Namespace, Value, PackageName, PackagePath)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, row['VariableName'], row['DataType'], row['Namespace'], row['Value'], row['PackageName'], row['PackagePath'])
            conn.commit()
            cursor.close()
            conn.close()
            print("Saved variable metadata to SQL Server")

    def analyze_single_connection_manager(self, connection_manager_path):
        import xml.dom.minidom as minidom

        doc = minidom.parse(connection_manager_path)
        root = doc.documentElement

        ns = {
            'DTS': 'www.microsoft.com/SqlServer/Dts'
        }

        def get_node_value(xpath):
            nodes = root.getElementsByTagNameNS(ns['DTS'], xpath)
            return nodes[0].nodeValue if nodes and nodes[0].firstChild else ""

        connection_string_name = ""
        connection_name = ""
        connection_id = ""
        connection_expression = ""
        connection_type = ""

        for node in root.getElementsByTagNameNS(ns['DTS'], 'ConnectionManager'):
            connection_string_name = node.getAttributeNS(ns['DTS'], 'ConnectionString')
            connection_name = node.getAttributeNS(ns['DTS'], 'ObjectName')
            connection_type = node.getAttributeNS(ns['DTS'], 'CreationName')
            connection_id = node.getAttributeNS(ns['DTS'], 'DTSID')

        for node in root.getElementsByTagNameNS(ns['DTS'], 'PropertyExpression'):
            name_attr = node.getAttributeNS(ns['DTS'], 'Name')
            value = node.firstChild.nodeValue if node.firstChild else ""
            connection_expression += f"{name_attr} : {value} "

        metadata = {
            'Connections': [
                {
                    'ConnectionName': connection_name,
                    'ConnectionString': connection_string_name,
                    'ConnectionExpressions': connection_expression,
                    'ConnectionType': connection_type,
                    'ConnectionID': connection_id,
                    'IsProjectConnection': "1"
                }
            ],
            'PackagePath': os.path.dirname(connection_manager_path),
            'PackageName': os.path.basename(connection_manager_path),
        }

        self.save_connections_metadata(metadata, self.PackageDetailsFilePath)

    def analyze_param_manager(self, param_file):
        tree = ET.parse(param_file)
        root = tree.getroot()
        ns = {'SSIS': 'www.microsoft.com/SqlServer/SSIS'}
        metadata = {
            'ProjectParameterDetails': [],
            'PackagePath': os.path.dirname(param_file),
            'PackageName': os.path.basename(param_file),
        }
        parameters = root.findall(".//SSIS:Parameter", ns)
        type_map = {
            "3": "Boolean", "6": "Byte", "16": "DateTime", "15": "Decimal",
            "14": "Double", "7": "Int16", "9": "Int32", "11": "Int64",
            "5": "SByte", "13": "Single", "18": "String", "10": "Unit32",
            "12": "Unit64"
        }
        for parameter in parameters:
            parameter_name = parameter.attrib.get('{www.microsoft.com/SqlServer/SSIS}Name')
            value_node = parameter.find("SSIS:Properties/SSIS:Property[@SSIS:Name='Value']", ns)
            datatype_node = parameter.find("SSIS:Properties/SSIS:Property[@SSIS:Name='DataType']", ns)
            value = value_node.text if value_node is not None else None
            datatype = datatype_node.text if datatype_node is not None else None
            datatype = type_map.get(datatype, datatype)
            metadata['ProjectParameterDetails'].append({
                'ParameterName': parameter_name,
                'DataType': datatype,
                'Value': value
            })
        self.save_project_parameter_metadata(metadata, self.PackageDetailsFilePath)

    def save_project_parameter_metadata(self, metadata, file_path):
        print(f"Saving project parameter metadata: {metadata}")

    def save_connections_metadata(self, metadata, file_path):
        print(f"Saving connection metadata: {metadata}")

    def log_error(self, file_path, exception):
        print(f"Error processing {file_path}: {str(exception)}")
        traceback.print_exc()

    def save_update_connection_name(self, file_path):
        pass


class Program:
    @staticmethod
    def delete_all_files_in_directory(directory_path):
        try:
            if os.path.exists(directory_path):
                for file_name in os.listdir(directory_path):
                    file_path = os.path.join(directory_path, file_name)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
        except Exception as ex:
            print(f"An error occurred: {str(ex)}")


def main():
    connection_string = ""
    output_folder = ""
    package_folder = input("Enter the Package Folder path:\n")
    data_save_type = input("Enter the Data Save Type (SQL or EXCEL):\n")

    if data_save_type.upper() == "SQL":
        connection_string = input("Enter the Connection String:\n")
    elif data_save_type.upper() == "EXCEL":
        output_folder = input("Enter the Output Folder path:\n")
    else:
        print("Wrong Input")
        threading.Event().wait(5)
        return

    package_analysis_file_path = os.path.join(output_folder, "PackageAnalysisResult.xlsx")
    dataflow_file_path = output_folder
    package_details_file_path = os.path.join(output_folder, "PackageDetails.xlsx")

    if data_save_type.upper() == "EXCEL":
        Program.delete_all_files_in_directory(dataflow_file_path)

    analyzer = SSISPackageAnalyzer(
        package_folder,
        connection_string,
        package_analysis_file_path,
        dataflow_file_path,
        package_details_file_path,
        data_save_type
    )
    analyzer.analyze_all_packages()
    print("Running...")


if __name__ == "__main__":
    main()
