import os
import time
import openpyxl
import pyodbc
import threading
import traceback
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import pandas as pd
from openpyxl import load_workbook,Workbook
from xml.dom import minidom
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timedelta
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

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
        if self.DataSaveType.upper() == "SQL":
            try:
                conn = pyodbc.connect(self._connection_string)
                cursor = conn.cursor()
                connection_query = """
                    TRUNCATE TABLE PackageAnalysisResults;
                    TRUNCATE TABLE PackageTaskDetails;
                    TRUNCATE TABLE PackageConnectionDetails;
                    TRUNCATE TABLE PackageContainerDetails;
                    TRUNCATE TABLE ProjectParameterDetails;
                    TRUNCATE TABLE PackageVariableParameterDetails;
                    TRUNCATE TABLE DataFlowTaskMappingDetails;
                    TRUNCATE TABLE PrecedenceConstraintDetails;
                    TRUNCATE TABLE EventTaskDetails;
                """
                cursor.execute(connection_query)
                conn.commit()
                cursor.close()
                conn.close()
                print("Truncated all metadata tables.")
            except Exception as e:
                self.log_error("SQL Truncate", e)

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

    def measure_package_performance(self, package_obj):
        """
        Executes a mock package and returns the execution time as a timedelta object.
        Assumes 'package_obj' has an 'execute()' method.
        """
        start_time = time.time()
        package_obj.execute()  # This should be your actual package processing logic
        end_time = time.time()
        return end_time - start_time  # Returns seconds as float

    @staticmethod
    def does_workbook_exist(file_path):
        """
        Checks if an Excel workbook exists at the specified path.
        Returns True if exists, otherwise False.
        """
        return os.path.isfile(file_path)

    def save_package_metadata(self, result, analysis_file_path, details_file_path):
        complexity = ""
        complexity_count = (
            len(result.Tasks) + len(result.Foreachtasks) + len(result.Seqtasks) +
            len(result.Forlooptasks) + len(result.Containers) + self.container_count + self.ComponentCount
        )

        if complexity_count <= 5:
            complexity = "Simple"
        elif 5 < complexity_count <= 10:
            complexity = "Medium"
        elif complexity_count > 10:
            complexity = "Complex"
        else:
            complexity = "Simple"

        if self.DataSaveType.upper() == "EXCEL":
            workbook_exists = os.path.exists(analysis_file_path)
            if workbook_exists:
                workbook = load_workbook(analysis_file_path)
            else:
                workbook = Workbook()

            sheet_name = "PackageAnalysisResults"
            if sheet_name not in workbook.sheetnames:
                sheet = workbook.create_sheet(sheet_name)
                sheet.append([
                    "PackageName", "PackagePath", "TasksCount", "ConnectionsCount", "ContainerCount",
                    "ComponentCount", "ExecutionTime", "CreatedDate", "CreatedBy", "Complexity"
                ])
            else:
                sheet = workbook[sheet_name]

            row = [
                result.PackageName,
                result.PackagePath,
                len(result.Tasks) + len(result.Foreachtasks) + len(result.Seqtasks) + len(result.Forlooptasks),
                len(result.Connections),
                len(result.Containers) + self.container_count,
                self.ComponentCount,
                result.ExecutionTime,
                result.CreatedDate,
                result.CreatedBy,
                complexity
            ]
            sheet.append(row)
            workbook.save(analysis_file_path)

            # Saving Variables to 'PackageVariableParameterDetails'
            workbook_details = load_workbook(details_file_path) if os.path.exists(details_file_path) else Workbook()

            # 1. Variables
            var_sheet_name = "PackageVariableParameterDetails"
            if var_sheet_name not in workbook_details.sheetnames:
                sheet_vars = workbook_details.create_sheet(var_sheet_name)
                sheet_vars.append([
                    "PackageName", "PackagePath", "VariableOrParameterName", "DataType", "Value", "IsParameter"
                ])
            else:
                sheet_vars = workbook_details[var_sheet_name]

            for var in result.Variables:
                sheet_vars.append([
                    result.PackageName, result.PackagePath, var.Name, var.DataType, var.Value, var.IsParameter
                ])

            # 2. Connections
            conn_sheet_name = "PackageConnectionDetails"
            if conn_sheet_name not in workbook_details.sheetnames:
                sheet_conn = workbook_details.create_sheet(conn_sheet_name)
                sheet_conn.append([
                    "PackageName", "PackagePath", "ConnectionName", "ConnectionType",
                    "ConnectionExpressions", "ConnectionString", "ConnectionID", "IsProjectConnection"
                ])
            else:
                sheet_conn = workbook_details[conn_sheet_name]

            for conn in result.Connections:
                sheet_conn.append([
                    result.PackageName, result.PackagePath,
                    conn.ConnectionName, conn.ConnectionType,
                    conn.ConnectionExpressions, conn.ConnectionString,
                    conn.ConnectionID, conn.IsProjectConnection
                ])

            workbook_details.save(details_file_path)

        elif self.DataSaveType.upper() == "SQL":
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()

            insert_package = """
            INSERT INTO PackageAnalysisResults 
            (PackageName, CreatedDate, TaskCount, ConnectionCount, ExecutionTime, PackageFolder, ContainerCount, DTSXXML, CreatedBy, DataFlowTaskComponentCount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_package, (
                result.PackageName,
                result.CreatedDate,
                len(result.Tasks) + len(result.Foreachtasks) + len(result.Seqtasks) + len(result.Forlooptasks),
                len(result.Connections),
                result.ExecutionTime,
                result.PackagePath,
                len(result.Containers) + self.container_count,
                result.DTSXXML,
                result.CreatedBy,
                self.ComponentCount
            ))

            for var in result.Variables:
                insert_var = """
                INSERT INTO PackageVariableParameterDetails 
                (PackageName, VariableOrParameterName, DataType, Value, PackagePath, IsParameter)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_var, (
                    result.PackageName, var.Name, var.DataType, var.Value, result.PackagePath, var.IsParameter
                ))

            for conn in result.Connections:
                insert_conn = """
                INSERT INTO PackageConnectionDetails 
                (PackageName, ConnectionName, ConnectionType, PackagePath, ConnectionExpressions, ConnectionString, ConnectionDTSID, IsProjectConnection)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_conn, (
                    result.PackageName,
                    conn.ConnectionName,
                    conn.ConnectionType,
                    result.PackagePath,
                    conn.ConnectionExpressions,
                    conn.ConnectionString,
                    conn.ConnectionID,
                    conn.IsProjectConnection
                ))

            conn.commit()
            cursor.close()
            conn.close()

    def save_dataflow_metadata(self, result, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            for dataflow in result.DataFlowTaskDetails:
                dataflow_file = os.path.join(
                    file_path, dataflow.PackageName.replace(".dtsx", "_DFM.xlsx")
                )
                workbook_exists = os.path.exists(dataflow_file)

                if workbook_exists:
                    workbook = openpyxl.load_workbook(dataflow_file)
                else:
                    workbook = openpyxl.Workbook()
                    workbook.remove(workbook.active)

                sheet_name = "DataFlowTaskMappingDetails"
                if sheet_name not in workbook.sheetnames:
                    ws = workbook.create_sheet(sheet_name)
                    ws.append([
                        "PackageName", "PackagePath", "TaskName", "ColumnName", "ColumnType", "DataType",
                        "ComponentName", "DataConversion", "ComponentPropertyDetails",
                        "ColumnPropertyDetails", "isEventHandler"
                    ])
                else:
                    ws = workbook[sheet_name]

                # Check for existing record
                exists = False
                for row in ws.iter_rows(min_row=2, values_only=True):
                    if (
                        row[0] == dataflow.PackageName and
                        row[1] == dataflow.PackagePath and
                        row[2] == dataflow.TaskName and
                        row[3] == dataflow.ColumnName and
                        row[4] == dataflow.ColumnType and
                        row[5] == dataflow.DataType and
                        row[6] == dataflow.componentName and
                        row[7] == dataflow.DataConversion and
                        row[8] == dataflow.componentPropertyDetails and
                        row[9] == dataflow.ColumnPropertyDetails and
                        row[10] == dataflow.isEventHandler
                    ):
                    exists = True
                    break

                if not exists:
                    ws.append([
                        dataflow.PackageName,
                        dataflow.PackagePath,
                        dataflow.TaskName,
                        dataflow.ColumnName,
                        dataflow.ColumnType,
                        dataflow.DataType,
                        dataflow.componentName,
                        dataflow.DataConversion,
                        dataflow.componentPropertyDetails,
                        dataflow.ColumnPropertyDetails,
                        dataflow.isEventHandler
                    ])
                    workbook.save(dataflow_file)

        elif self.DataSaveType.upper() == "SQL":
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()

            for dataflow in result.DataFlowTaskDetails:
                insert_query = """
                    INSERT INTO DataFlowTaskMappingDetails (
                        PackageName, TaskName, ColumnName, DataType, ComponentName, DataConversion, PackagePath, 
                        ColumnType, isEventHandler, ComponentPropertyDetails, ColumnPropertyDetails
                    )
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM DataFlowTaskMappingDetails
                        WHERE ISNULL(ColumnName, '') = ISNULL(?, '') AND ISNULL(DataType, '') = ISNULL(?, '')
                        AND ISNULL(PackageName, '') = ISNULL(?, '') AND ISNULL(PackagePath, '') = ISNULL(?, '')
                        AND ISNULL(ColumnType, '') = ISNULL(?, '') AND ISNULL(ComponentName, '') = ISNULL(?, '')
                        AND ISNULL(TaskName, '') = ISNULL(?, '') AND ISNULL(ComponentPropertyDetails, '') = ISNULL(?, '')
                        AND ISNULL(ColumnPropertyDetails, '') = ISNULL(?, '')
                    )
                """
                values = [
                    dataflow.PackageName,
                    dataflow.TaskName,
                    dataflow.ColumnName,
                    dataflow.DataType,
                    dataflow.componentName,
                    dataflow.DataConversion,
                    dataflow.PackagePath,
                    dataflow.ColumnType,
                    dataflow.isEventHandler,
                    dataflow.componentPropertyDetails,
                    dataflow.ColumnPropertyDetails,
                    # WHERE NOT EXISTS values
                    dataflow.ColumnName,
                    dataflow.DataType,
                    dataflow.PackageName,
                    dataflow.PackagePath,
                    dataflow.ColumnType,
                    dataflow.componentName,
                    dataflow.TaskName,
                    dataflow.componentPropertyDetails,
                    dataflow.ColumnPropertyDetails
                ]
                cursor.execute(insert_query, values)

            conn.commit()
            cursor.close()
            conn.close()

    def save_precedence_constraint_metadata(self, result, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            workbook_exists = os.path.exists(file_path)
            for precedence in result.PrecedenceConstraintDetails:
                if workbook_exists:
                    workbook = openpyxl.load_workbook(file_path)
                else:
                    workbook = openpyxl.Workbook()
                    workbook.remove(workbook.active)

                sheet_name = "PrecedenceConstraintDetails"
                if sheet_name not in workbook.sheetnames:
                    ws = workbook.create_sheet(sheet_name)
                    ws.append([
                        "PackageName", "PackagePath", "PrecedenceConstraintFrom", "PrecedenceConstraintTo",
                        "PrecedenceConstraintValue", "PrecedenceConstraintExpression",
                        "PrecedenceConstraintLogicalAnd", "PrecedenceConstraintEvalOP", "ContainerName"
                    ])
                else:
                    ws = workbook[sheet_name]

                # Check if record exists
                exists = False
                for row in ws.iter_rows(min_row=2, values_only=True):
                    if (
                        row[0] == precedence.PackageName and
                        row[1] == precedence.PackagePath and
                        row[2] == precedence.PrecedenceConstraintFrom and
                        row[3] == precedence.PrecedenceConstraintTo and
                        row[4] == precedence.PrecedenceConstraintValue and
                        row[5] == precedence.PrecedenceConstraintExpression and
                        row[6] == precedence.PrecedenceConstraintLogicalAnd and
                        row[7] == precedence.PrecedenceConstraintEvalOP and
                        row[8] == precedence.ContainerName
                    ):
                        exists = True
                        break

                if not exists:
                    ws.append([
                        precedence.PackageName, precedence.PackagePath,
                        precedence.PrecedenceConstraintFrom, precedence.PrecedenceConstraintTo,
                        precedence.PrecedenceConstraintValue, precedence.PrecedenceConstraintExpression,
                        precedence.PrecedenceConstraintLogicalAnd, precedence.PrecedenceConstraintEvalOP,
                        precedence.ContainerName
                    ])
                    workbook.save(file_path)

        elif self.DataSaveType.upper() == "SQL":
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()
            for precedence in result.PrecedenceConstraintDetails:
                insert_query = """
                    INSERT INTO PrecedenceConstraintDetails (
                        PackageName, PrecedenceConstraintFrom, PrecedenceConstraintTo, 
                        PrecedenceConstraintValue, PrecedenceConstraintExpression, PrecedenceConstraintLogicalAnd,
                        PrecedenceConstraintEvalOP, ContainerName, PackagePath
                    )
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM PrecedenceConstraintDetails
                        WHERE PackageName = ? AND PrecedenceConstraintFrom = ? AND PrecedenceConstraintTo = ?
                        AND PrecedenceConstraintValue = ? AND PrecedenceConstraintExpression = ?
                        AND PrecedenceConstraintLogicalAnd = ? AND PrecedenceConstraintEvalOP = ?
                        AND ContainerName = ? AND PackagePath = ?
                    )
                """
                values = [
                    precedence.PackageName, precedence.PrecedenceConstraintFrom, precedence.PrecedenceConstraintTo,
                    precedence.PrecedenceConstraintValue, precedence.PrecedenceConstraintExpression,
                    precedence.PrecedenceConstraintLogicalAnd, precedence.PrecedenceConstraintEvalOP,
                    precedence.ContainerName, precedence.PackagePath,
                    # For WHERE NOT EXISTS part
                    precedence.PackageName, precedence.PrecedenceConstraintFrom, precedence.PrecedenceConstraintTo,
                    precedence.PrecedenceConstraintValue, precedence.PrecedenceConstraintExpression,
                    precedence.PrecedenceConstraintLogicalAnd, precedence.PrecedenceConstraintEvalOP,
                    precedence.ContainerName, precedence.PackagePath
                ]
                cursor.execute(insert_query, values)
            conn.commit()
            cursor.close()
            conn.close()

    def save_event_metadata(self, result, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            workbook_exists = os.path.exists(file_path)
            for task in result.ExtractTaskDetails:
                if not task.TaskName:
                    continue

                if workbook_exists:
                    workbook = load_workbook(file_path)
                else:
                    workbook = openpyxl.Workbook()
                    workbook.remove(workbook.active)

                sheet_name = "EventHandlerTaskDetails"
                if sheet_name not in workbook.sheetnames:
                    ws = workbook.create_sheet(sheet_name)
                    ws.append([
                        "PackageName", "PackagePath", "EventHandlerName", "EventHandlerType", "EventType",
                        "TaskName", "TaskType", "ContainerName", "ContainerType", "ContainerExpression",
                        "TaskConnectionName", "SqlQuery", "Variables", "Parameters", "Expressions",
                        "DataFlowDaskSourceName", "DataFlowTaskSourceType", "DataFlowTaskTargetName", "DataFlowTaskTargetType",
                        "DataFlowTaskTargetTable", "DataFlowDaskSourceConnectionName", "DataFlowDaskTargetConnectionName",
                        "SendMailTaskDetails", "ResultSetDetails", "TaskComponentDetails"
                    ])
                ws = workbook[sheet_name]
                ws.append([
                    task.PackageName, task.PackagePath, task.EventHandlerName, task.EventHandlerType, task.EventType,
                    task.TaskName, task.TaskType, task.ContainerName, task.ContainerType, task.ContainerExpression,
                    task.ConnectionName, task.TaskSqlQuery, task.Variables, task.Parameters, task.Expressions,
                    task.SourceComponent, task.SourceType, task.TargetComponent, task.TargetType,
                    task.TargetTable, task.SourceConnectionName, task.TargetConnectionName,
                    task.SendMailTask, task.ResultSetDetails, task.TaskComponentDetails
                ])
                workbook.save(file_path)

        elif self.DataSaveType.upper() == "SQL":
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()
            for task in result.ExtractTaskDetails:
                if not task.TaskName:
                    continue
                insert_query = """
                    INSERT INTO EventTaskDetails (
                        PackageName, TaskName, TaskType, SqlQuery, ContainerName, PackagePath,
                        Variables, Parameters, Expressions, DataFlowDaskSourceName,
                        DataFlowTaskSourceType, DataFlowTaskTargetName, DataFlowTaskTargetType,
                        DataFlowTaskTargetTable, SendMailTaskDetails, ResultSetDetails,
                        ContainerType, ContainerExpression, EventHandlerName, EventHandlerType,
                        EventType, DataFlowDaskSourceConnectionName, DataFlowDaskTargetConnectionName,
                        TaskConnectionName, TaskComponentDetails
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_query, (
                    task.PackageName, task.TaskName, task.TaskType, task.TaskSqlQuery, task.ContainerName,
                    task.PackagePath, task.Variables, task.Parameters, task.Expressions, task.SourceComponent,
                    task.SourceType, task.TargetComponent, task.TargetType, task.TargetTable, task.SendMailTask,
                    task.ResultSetDetails, task.ContainerType, task.ContainerExpression, task.EventHandlerName,
                    task.EventHandlerType, task.EventType, task.SourceConnectionName, task.TargetConnectionName,
                    task.ConnectionName, task.TaskComponentDetails
                ))
            conn.commit()
            cursor.close()
            conn.close()

    def save_package_task_metadata(self, result, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            tasks = result.get("ExtractTaskDetails", [])
            workbook_exists = os.path.exists(file_path)

            for task in tasks:
                if task.get("TaskName"):
                    wb = load_workbook(file_path) if workbook_exists else Workbook()
                    if not wb.sheetnames:
                        wb.remove(wb.active)

                    sheet_name = "PackageTaskDetails"
                    if sheet_name in wb.sheetnames:
                        ws = wb[sheet_name]
                    else:
                        ws = wb.create_sheet(sheet_name)
                        ws.append([
                            "PackageName", "PackagePath", "TaskName", "TaskType", "ContainerName",
                            "TaskConnectionName", "SqlQuery", "Variables", "Parameters", "Expressions",
                            "DataFlowDaskSourceName", "DataFlowTaskSourceType", "DataFlowTaskTargetName",
                            "DataFlowTaskTargetType", "DataFlowTaskTargetTable", "DataFlowDaskSourceConnectionName",
                            "DataFlowDaskTargetConnectionName", "ResultSetDetails", "TaskComponentDetails"
                        ])

                    rows = list(ws.iter_rows(min_row=2, values_only=True))
                    record_exists = any(
                        row[0] == task.get("PackageName") and
                        row[1] == task.get("PackagePath") and
                        row[2] == task.get("TaskName") and
                        row[3] == task.get("TaskType") and
                        row[4] == task.get("ContainerName")
                        for row in rows
                    )

                    if not record_exists:
                        ws.append([
                            task.get("PackageName"), task.get("PackagePath"), task.get("TaskName"),
                            task.get("TaskType"), task.get("ContainerName"), task.get("ConnectionName"),
                            task.get("TaskSqlQuery"), task.get("Variables"), task.get("Parameters"),
                            task.get("Expressions"), task.get("SourceComponent"), task.get("SourceType"),
                            task.get("TargetComponent"), task.get("TargetType"), task.get("TargetTable"),
                            task.get("SourceConnectionName"), task.get("TargetConnectionName"),
                            task.get("ResultSetDetails"), task.get("TaskComponentDetails")
                        ])
                        wb.save(file_path)

                if task.get("ContainerName"):
                    wb = load_workbook(file_path) if os.path.exists(file_path) else Workbook()
                    if not wb.sheetnames:
                        wb.remove(wb.active)
                    sheet_name = "PackageContainerDetails"
                    if sheet_name in wb.sheetnames:
                        ws = wb[sheet_name]
                    else:
                        ws = wb.create_sheet(sheet_name)
                        ws.append([
                            "PackageName", "PackagePath", "ContainerName",
                            "ContainerType", "ContainerExpressions", "ContainerEnumerator"
                        ])

                    rows = list(ws.iter_rows(min_row=2, values_only=True))
                    record_exists = any(
                        row[0] == task.get("PackageName") and
                        row[1] == task.get("PackagePath") and
                        row[2] == task.get("ContainerName") and
                        row[3] == task.get("ContainerType") and
                        row[4] == task.get("ContainerExpression") and
                        row[5] == task.get("ContainerEnum")
                        for row in rows
                    )

                    if not record_exists:
                        ws.append([
                            task.get("PackageName"), task.get("PackagePath"), task.get("ContainerName"),
                            task.get("ContainerType"), task.get("ContainerExpression"), task.get("ContainerEnum")
                        ])
                        wb.save(file_path)

        elif self.DataSaveType.upper() == "SQL":
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()

            for task in result.get("ExtractTaskDetails", []):
                if task.get("TaskName"):
                    task_query = """
                        INSERT INTO PackageTaskDetails (
                            PackageName, TaskName, TaskType, SqlQuery, ContainerName, PackagePath,
                            Variables, Parameters, Expressions, DataFlowDaskSourceName, DataFlowTaskSourceType,
                            DataFlowTaskTargetName, DataFlowTaskTargetType, DataFlowTaskTargetTable,
                            ResultSetDetails, DataFlowDaskSourceConnectionName,
                            DataFlowDaskTargetConnectionName, TaskConnectionName, TaskComponentDetails
                        )
                        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM PackageTaskDetails
                            WHERE ContainerName = ? AND PackageName = ? AND PackagePath = ? AND TaskName = ?
                        )
                    """
                    values = [
                        task.get("PackageName"), task.get("TaskName"), task.get("TaskType"),
                        task.get("TaskSqlQuery"), task.get("ContainerName"), task.get("PackagePath"),
                        task.get("Variables"), task.get("Parameters"), task.get("Expressions"),
                        task.get("SourceComponent"), task.get("SourceType"), task.get("TargetComponent"),
                        task.get("TargetType"), task.get("TargetTable"), task.get("ResultSetDetails"),
                        task.get("SourceConnectionName"), task.get("TargetConnectionName"),
                        task.get("ConnectionName"), task.get("TaskComponentDetails"),
                        task.get("ContainerName"), task.get("PackageName"), task.get("PackagePath"), task.get("TaskName")
                    ]
                    cursor.execute(task_query, values)

                if task.get("ContainerName"):
                    container_query = """
                        INSERT INTO PackageContainerDetails (
                            PackageName, ContainerName, ContainerType,
                            ContainerExpressions, ContainerEnumerator, PackagePath
                        )
                        SELECT ?, ?, ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM PackageContainerDetails
                            WHERE ContainerName = ? AND ContainerType = ? AND PackageName = ?
                            AND PackagePath = ? AND ContainerExpressions = ISNULL(?, '')
                            AND ContainerEnumerator = ISNULL(?, '')
                        )
                    """
                    values = [
                        task.get("PackageName"), task.get("ContainerName"), task.get("ContainerType"),
                        task.get("ContainerExpression"), task.get("ContainerEnum"), task.get("PackagePath"),
                        task.get("ContainerName"), task.get("ContainerType"), task.get("PackageName"),
                        task.get("PackagePath"), task.get("ContainerExpression"), task.get("ContainerEnum")
                    ]
                    cursor.execute(container_query, values)

            conn.commit()
            cursor.close()
            conn.close()
            print("Saved Package Task and Container metadata to SQL Server.")

    def save_project_parameter_metadata(self, result, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            for param_info in result.get("ProjectParameterDetails", []):
                if not result.get("PackageName"):
                    continue

                if os.path.exists(file_path):
                    wb = load_workbook(file_path)
                else:
                    wb = Workbook()
                    wb.remove(wb.active)

                if "ProjectParameterDetails" in wb.sheetnames:
                    ws = wb["ProjectParameterDetails"]
                else:
                    ws = wb.create_sheet("ProjectParameterDetails")
                    ws.append(["ProjectPath", "ParameterName", "ParameterValue", "ParameterDataType"])

                row = [
                    result.get("PackagePath", ""),
                    param_info.get("ParameterName", ""),
                    param_info.get("Value", ""),
                    param_info.get("DataType", "")
                ]
                ws.append(row)
                wb.save(file_path)

            print(f"Saved project parameters to Excel: {file_path}")

        elif self.DataSaveType.upper() == "SQL":
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()

            for param_info in result.get("ProjectParameterDetails", []):
                if not result.get("PackageName"):
                    continue

                cursor.execute("""
                    INSERT INTO ProjectParameterDetails (
                        ParameterName, ParameterValue, ParameterDataType, ProjectPath
                    ) VALUES (?, ?, ?, ?)
                """, (
                    param_info.get("ParameterName", ""),
                    param_info.get("Value", ""),
                    param_info.get("DataType", ""),
                    result.get("PackagePath", "")
                ))

            conn.commit()
            cursor.close()
            conn.close()
            print("Saved project parameters to SQL Server.")

    def save_connections_metadata(self, result, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            for conn in result.get("Connections", []):
                if os.path.exists(file_path):
                    wb = load_workbook(file_path)
                else:
                    wb = Workbook()
                    wb.remove(wb.active)

                if "PackageConnectionDetails" in wb.sheetnames:
                    ws = wb["PackageConnectionDetails"]
                else:
                    ws = wb.create_sheet("PackageConnectionDetails")
                    ws.append([
                        "PackageName", "PackagePath", "ConnectionName", "ConnectionType",
                        "ConnectionExpressions", "ConnectionString", "ConnectionID", "IsProjectConnection"
                    ])

                row = [
                    result.get("PackageName", ""),
                    result.get("PackagePath", ""),
                    conn.get("ConnectionName", ""),
                    conn.get("ConnectionType", ""),
                    conn.get("ConnectionExpressions", ""),
                    conn.get("ConnectionString", ""),
                    conn.get("ConnectionID", ""),
                    conn.get("IsProjectConnection", "")
                ]
                ws.append(row)
                wb.save(file_path)

            print(f"Saved connection metadata to Excel: {file_path}")

        elif self.DataSaveType.upper() == "SQL":
            conn_db = pyodbc.connect(self._connection_string)
            cursor = conn_db.cursor()

            for conn_info in result.get("Connections", []):
                cursor.execute("""
                    INSERT INTO PackageConnectionDetails (
                        PackageName, ConnectionName, ConnectionType, PackagePath, 
                        ConnectionExpressions, ConnectionString, ConnectionDTSID, IsProjectConnection
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    result.get("PackageName", ""),
                    conn_info.get("ConnectionName", ""),
                    conn_info.get("ConnectionType", ""),
                    result.get("PackagePath", ""),
                    conn_info.get("ConnectionExpressions", ""),
                    conn_info.get("ConnectionString", ""),
                    conn_info.get("ConnectionID", ""),
                    conn_info.get("IsProjectConnection", "")
                ))

            conn_db.commit()
            cursor.close()
            conn_db.close()
            print("Saved connection metadata to SQL Server.")

    def log_error(self, file_path, exception):
        print(f"Error processing {file_path}: {str(exception)}")
        traceback.print_exc()

    def save_update_connection_name(self, file_path):
        if self.DataSaveType.upper() == "EXCEL":
            wb = load_workbook(file_path)

            sheet1 = wb["PackageTaskDetails"]
            sheet2 = wb["PackageConnectionDetails"]
            sheet3 = wb["EventHandlerTaskDetails"]

            for row2 in range(2, sheet2.max_row + 1):
                conn_pkg_path = sheet2.cell(row=row2, column=2).value
                conn_name = sheet2.cell(row=row2, column=3).value
                conn_dtsid = sheet2.cell(row=row2, column=7).value

                for row1 in range(2, sheet1.max_row + 1):
                    task_pkg_path = sheet1.cell(row=row1, column=2).value
                    if conn_pkg_path == task_pkg_path:
                        if conn_dtsid == sheet1.cell(row=row1, column=6).value:
                            sheet1.cell(row=row1, column=6).value = conn_name
                        elif conn_dtsid == sheet1.cell(row=row1, column=16).value:
                            sheet1.cell(row=row1, column=16).value = conn_name
                        elif conn_dtsid == sheet1.cell(row=row1, column=17).value:
                            sheet1.cell(row=row1, column=17).value = conn_name

                for row3 in range(2, sheet3.max_row + 1):
                    handler_pkg_path = sheet3.cell(row=row3, column=2).value
                    if conn_pkg_path == handler_pkg_path:
                        if conn_dtsid == sheet3.cell(row=row3, column=11).value:
                            sheet3.cell(row=row3, column=11).value = conn_name
                        elif conn_dtsid == sheet3.cell(row=row3, column=21).value:
                            sheet3.cell(row=row3, column=21).value = conn_name
                        elif conn_dtsid == sheet3.cell(row=row3, column=22).value:
                            sheet3.cell(row=row3, column=22).value = conn_name

            wb.save(file_path)
            print("Connection names updated in Excel.")

        elif self.DataSaveType.upper() == "SQL":
            conn = pyodbc.connect(self._connection_string)
            cursor = conn.cursor()

            connection_query = """
            UPDATE task SET task.TaskConnectionName = conn.ConnectionName
            FROM PackageTaskDetails task
            INNER JOIN PackageConnectionDetails conn WITH (NOLOCK)
                ON conn.PackagePath = task.PackagePath
                AND task.TaskConnectionName = conn.ConnectionDTSID
            WHERE ISNULL(task.TaskConnectionName, '') <> '';

            UPDATE task SET 
                task.DataFlowDaskSourceConnectionName = sconn.ConnectionName, 
                task.DataFlowDaskTargetConnectionName = tconn.ConnectionName
            FROM PackageTaskDetails task
            INNER JOIN PackageConnectionDetails sconn WITH (NOLOCK)
                ON sconn.PackagePath = task.PackagePath
                AND task.DataFlowDaskSourceConnectionName = sconn.ConnectionDTSID
            INNER JOIN PackageConnectionDetails tconn WITH (NOLOCK)
                ON tconn.PackagePath = task.PackagePath
                AND task.DataFlowDaskTargetConnectionName = tconn.ConnectionDTSID
            WHERE ISNULL(task.DataFlowDaskSourceConnectionName, '') <> '';

            UPDATE task SET task.TaskConnectionName = conn.ConnectionName
            FROM EventTaskDetails task
            INNER JOIN PackageConnectionDetails conn WITH (NOLOCK)
                ON conn.PackagePath = task.PackagePath
                AND task.TaskConnectionName = conn.ConnectionDTSID
            WHERE ISNULL(task.TaskConnectionName, '') <> '';

            UPDATE task SET 
                task.DataFlowDaskSourceConnectionName = sconn.ConnectionName,
                task.DataFlowDaskTargetConnectionName = tconn.ConnectionName
            FROM EventTaskDetails task
            INNER JOIN PackageConnectionDetails sconn WITH (NOLOCK)
                ON sconn.PackagePath = task.PackagePath
                AND task.DataFlowDaskSourceConnectionName = sconn.ConnectionDTSID
            INNER JOIN PackageConnectionDetails tconn WITH (NOLOCK)
                ON tconn.PackagePath = task.PackagePath
                AND task.DataFlowDaskTargetConnectionName = tconn.ConnectionDTSID
            WHERE ISNULL(task.DataFlowDaskSourceConnectionName, '') <> '';

            -- Reset precedence constraints
            UPDATE task SET
                task.ONSuccessPrecedenceConstrainttoTask = '',
                task.ONSuccessPrecedenceConstraintExpression = '',
                task.ONSuccessPrecedenceConstraintEvalOP = '',
                task.ONSuccessPrecedenceConstraintLogicalAnd = '',
                task.ONFailurePrecedenceConstrainttoTask = '',
                task.ONFailurePrecedenceConstraintExpression = '',
                task.ONFailurePrecedenceConstraintEvalOP = '',
                task.ONFailurePrecedenceConstraintLogicalAnd = '',
                task.ONCompletionPrecedenceConstrainttoTask = '',
                task.ONCompletionPrecedenceConstraintExpression = '',
                task.ONCompletionPrecedenceConstraintEvalOP = '',
                task.ONCompletionPrecedenceConstraintLogicalAnd = ''
            FROM PackageTaskDetails task;

            -- Apply precedence constraints (Success, Failure, Completion)
            UPDATE task SET
                task.ONSuccessPrecedenceConstrainttoTask = pcd.PrecedenceConstraintto,
                task.ONSuccessPrecedenceConstraintExpression = pcd.PrecedenceConstraintExpression,
                task.ONSuccessPrecedenceConstraintEvalOP = pcd.PrecedenceConstraintEvalOP,
                task.ONSuccessPrecedenceConstraintLogicalAnd = pcd.PrecedenceConstraintLogicalAnd
            FROM PackageTaskDetails task
            INNER JOIN PrecedenceConstraintDetails pcd WITH (NOLOCK)
                ON pcd.PrecedenceConstraintFrom = task.TaskName
                AND pcd.PackageName = task.PackageName
                AND pcd.PackagePath = task.PackagePath
                AND ISNULL(pcd.ContainerName, '') = ISNULL(task.ContainerName, '')
            WHERE pcd.PrecedenceConstraintValue = 'Success';

            UPDATE task SET
                task.ONFailurePrecedenceConstrainttoTask = pcd.PrecedenceConstraintto,
                task.ONFailurePrecedenceConstraintExpression = pcd.PrecedenceConstraintExpression,
                task.ONFailurePrecedenceConstraintEvalOP = pcd.PrecedenceConstraintEvalOP,
                task.ONFailurePrecedenceConstraintLogicalAnd = pcd.PrecedenceConstraintLogicalAnd
            FROM PackageTaskDetails task
            INNER JOIN PrecedenceConstraintDetails pcd WITH (NOLOCK)
                ON pcd.PrecedenceConstraintFrom = task.TaskName
                AND pcd.PackageName = task.PackageName
                AND pcd.PackagePath = task.PackagePath
                AND ISNULL(pcd.ContainerName, '') = ISNULL(task.ContainerName, '')
            WHERE pcd.PrecedenceConstraintValue = 'Failure';

            UPDATE task SET
                task.ONCompletionPrecedenceConstrainttoTask = pcd.PrecedenceConstraintto,
                task.ONCompletionPrecedenceConstraintExpression = pcd.PrecedenceConstraintExpression,
                task.ONCompletionPrecedenceConstraintEvalOP = pcd.PrecedenceConstraintEvalOP,
                task.ONCompletionPrecedenceConstraintLogicalAnd = pcd.PrecedenceConstraintLogicalAnd
            FROM PackageTaskDetails task
            INNER JOIN PrecedenceConstraintDetails pcd WITH (NOLOCK)
                ON pcd.PrecedenceConstraintFrom = task.TaskName
                AND pcd.PackageName = task.PackageName
                AND pcd.PackagePath = task.PackagePath
                AND ISNULL(pcd.ContainerName, '') = ISNULL(task.ContainerName, '')
            WHERE pcd.PrecedenceConstraintValue = 'Completion';

            -- Set Complexity Classification
            UPDATE PA SET PA.Complexcity = CASE 
                WHEN Final.TaskCount + Final.ContainerCount + Final.ComponentCount < 5 THEN 'Simple'
                WHEN Final.TaskCount + Final.ContainerCount + Final.ComponentCount BETWEEN 5 AND 10 THEN 'Medium'
                WHEN Final.TaskCount + Final.ContainerCount + Final.ComponentCount > 10 THEN 'Complex'
                ELSE 'Simple'
            END
            FROM PackageAnalysisResults PA
            LEFT JOIN (
                SELECT PackageName, PackagePath,
                       SUM(TaskCount) TaskCount,
                       SUM(ContainerCount) ContainerCount,
                       SUM(ComponentCount) ComponentCount
                FROM (
                    SELECT PT.PackageName, PT.PackagePath,
                           SUM(CASE WHEN TaskType <> 'ExecutePackageTask' THEN 1 ELSE 0 END) AS TaskCount,
                           0 AS ContainerCount,
                           0 AS ComponentCount
                    FROM PackageTaskDetails PT
                    GROUP BY PT.PackageName, PT.PackagePath

                    UNION ALL

                    SELECT PT.PackageName, PT.PackagePath,
                           1 AS TaskCount, 0, 0
                    FROM PackageTaskDetails PT
                    WHERE TaskType = 'ExecutePackageTask'

                    UNION ALL

                    SELECT PC.PackageName, PC.PackagePath,
                           0, 1, 0
                    FROM PackageContainerDetails PC
                    WHERE PC.ContainerType = 'Sequence'

                    UNION ALL

                    SELECT PC.PackageName, PC.PackagePath,
                           0, COUNT(1), 0
                    FROM PackageContainerDetails PC
                    WHERE PC.ContainerType <> 'Sequence'
                    GROUP BY PackageName, PackagePath

                    UNION ALL

                    SELECT PC.PackageName, PC.PackagePath,
                           0, 0, COUNT(DISTINCT ComponentName)
                    FROM DataFlowTaskMappingDetails PC
                    GROUP BY PackageName, PackagePath
                ) A
                GROUP BY PackageName, PackagePath
            ) Final ON Final.PackageName = PA.PackageName
            AND Final.PackagePath = PA.PackageFolder;
            """

            cursor.execute(connection_query)
            conn.commit()
            cursor.close()
            conn.close()
            print("Connection names updated in SQL.")

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
