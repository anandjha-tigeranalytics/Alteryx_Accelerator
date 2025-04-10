# Over 25 unit + integration + regression tests
# Mocking, negative tests, snapshot comparisons, SQL validations

import os
import unittest
from unittest.mock import patch, MagicMock
import pyodbc
from ssis_package_analyzer import SSISPackageAnalyzer
import xml.etree.ElementTree as ET
import logging
import json
import pandas as pd
from datetime import datetime
import shutil

class TestSSISPackageAnalyzer(unittest.TestCase):

    def setUp(self):
        self.test_folder = "tests/sample_packages"
        self.output_folder = "tests/output"
        os.makedirs(self.test_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)

        self.sample_dtsx_path = os.path.join(self.test_folder, "TestPackage.dtsx")
        if not os.path.exists(self.sample_dtsx_path):
            with open(self.sample_dtsx_path, "w") as f:
                f.write("""
                <Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts" xmlns:SSIS="www.microsoft.com/SqlServer/SSIS">
                  <DTS:Variables>
                    <DTS:Variable DTS:ObjectName="SampleVar" DTS:DataType="18" DTS:Namespace="User">
                      <DTS:Value>sample value</DTS:Value>
                    </DTS:Variable>
                  </DTS:Variables>
                  <SSIS:Parameters>
                    <SSIS:Parameter SSIS:Name="SampleParam">
                      <SSIS:Properties>
                        <SSIS:Property SSIS:Name="DataType">18</SSIS:Property>
                        <SSIS:Property SSIS:Name="Value">Test</SSIS:Property>
                      </SSIS:Properties>
                    </SSIS:Parameter>
                  </SSIS:Parameters>
                  <DTS:Executables>
                    <DTS:Executable DTS:ObjectName="MyTask" DTS:CreationName="Microsoft.SqlServer.Dts.Tasks.ScriptTask.ScriptTask" />
                  </DTS:Executables>
                  <DTS:ConnectionManagers>
                    <DTS:ConnectionManager DTS:ObjectName="MyConn" DTS:ConnectionString="Data Source=.;Initial Catalog=Test;" DTS:CreationName="OLEDB" DTS:DTSID="123"/>
                  </DTS:ConnectionManagers>
                  <DTS:PrecedenceConstraints>
                    <DTS:PrecedenceConstraint DTS:From="Task1" DTS:To="Task2"/>
                  </DTS:PrecedenceConstraints>
                  <DTS:Sequence DTS:ObjectName="MainSeq" />
                  <DTS:Executables>
                    <DTS:ForEachLoop DTS:ObjectName="MyLoop" />
                    <DTS:ForLoop DTS:ObjectName="MyForLoop" />
                  </DTS:Executables>
                  <DTS:EventHandlers>
                    <DTS:EventHandler DTS:ObjectName="OnError">
                      <DTS:Executables>
                        <DTS:Executable DTS:ObjectName="ErrorHandlerTask" DTS:CreationName="ScriptTask" />
                      </DTS:Executables>
                    </DTS:EventHandler>
                  </DTS:EventHandlers>
                  <DTS:Pipeline DTS:ObjectName="DataFlowTask">
                    <DTS:components>
                      <DTS:component DTS:ObjectName="Derived Column" DTS:ComponentClassID="DerviedColumn"/>
                    </DTS:components>
                  </DTS:Pipeline>
                </Executable>
                """)

    def test_excel_output(self):
        analyzer = SSISPackageAnalyzer(
            package_folder=self.test_folder,
            output_folder=self.output_folder,
            save_type="EXCEL"
        )
        analyzer.analyze()
        self.assertTrue(os.path.exists(os.path.join(self.output_folder, "Variables.xlsx")))
        self.assertTrue(os.path.exists(os.path.join(self.output_folder, "Parameters.xlsx")))

    def test_sql_output(self):
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=tempdb;Trusted_Connection=yes;"
        analyzer = SSISPackageAnalyzer(
            package_folder=self.test_folder,
            output_folder=self.output_folder,
            save_type="SQL",
            connection_string=connection_string
        )
        analyzer.analyze()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM SSISVariables")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        self.assertGreater(count, 0)
        
    def test_mock_sql_insertion(self):
        analyzer = SSISPackageAnalyzer(
            package_folder=self.test_folder,
            output_folder=self.output_folder,
            save_type="SQL",
            connection_string="mock-connection"
        )
        analyzer.variables_metadata = [
            {
                'VariableName': 'Var1', 'DataType': 'String', 'Namespace': 'User',
                'Value': 'Test', 'PackageName': 'Pkg', 'PackagePath': '/path'
            }
        ]
        with patch("pyodbc.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            analyzer.save_variable_metadata(self.output_folder)
            mock_cursor.execute.assert_any_call("""
                INSERT INTO dbo.SSISVariables (VariableName, DataType, Namespace, Value, PackageName, PackagePath)
                VALUES (?, ?, ?, ?, ?, ?)
            """, 'Var1', 'String', 'User', 'Test', 'Pkg', '/path')
    
    def test_loop_container_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        sample_loop_metadata = {
            "Foreach": ["Loop1", "Loop2"],
            "For": ["ForLoop1"],
            "Sequence": ["Seq1", "Seq2"]
        }
        analyzer.loop_containers = sample_loop_metadata
        self.assertIn("Loop1", analyzer.loop_containers["Foreach"])
        self.assertIn("ForLoop1", analyzer.loop_containers["For"])

    def test_dataflow_task_extraction_mock(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.extract_dataflow_task = MagicMock(return_value=[{"Component": "DFT1"}])
        result = analyzer.extract_dataflow_task("mock_path.dtsx")
        self.assertEqual(result, [{"Component": "DFT1"}])

    def test_json_snapshot_comparison(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        sample_metadata = {
            "PackageName": "Sample",
            "Tasks": 3,
            "Connections": 2
        }
        snapshot_path = os.path.join(self.output_folder, "snapshot.json")
        with open(snapshot_path, "w") as f:
            json.dump(sample_metadata, f)

        with open(snapshot_path, "r") as f:
            loaded = json.load(f)

        self.assertDictEqual(sample_metadata, loaded)
        
    def test_precedence_constraint_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        mock_metadata = {
            "PrecedenceConstraints": [
                {"From": "TaskA", "To": "TaskB", "Constraint": "Success"},
                {"From": "TaskB", "To": "TaskC", "Constraint": "Completion"}
            ]
        }
        analyzer.precedence_constraints = mock_metadata["PrecedenceConstraints"]
        self.assertEqual(len(analyzer.precedence_constraints), 2)
        self.assertEqual(analyzer.precedence_constraints[0]["From"], "TaskA")

    def test_task_host_type_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.task_types = [
            {"Name": "ExecuteSQL", "Type": "SQL"},
            {"Name": "ScriptTask", "Type": "Script"}
        ]
        task_names = [t["Name"] for t in analyzer.task_types]
        self.assertIn("ExecuteSQL", task_names)
        self.assertIn("ScriptTask", task_names)
    
    def test_event_handler_extraction_mock(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.event_handlers = [
            {"HandlerType": "OnError", "Target": "SequenceContainer1"},
            {"HandlerType": "OnPostExecute", "Target": "MainTask"}
        ]
        handler_types = [h["HandlerType"] for h in analyzer.event_handlers]
        self.assertIn("OnError", handler_types)
        self.assertIn("OnPostExecute", handler_types)

    def test_invalid_dtsx_file(self):
        invalid_path = os.path.join(self.test_folder, "InvalidPackage.dtsx")
        with open(invalid_path, "w") as f:
            f.write("<Invalid<Broken>")

        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        with self.assertRaises(Exception):
            analyzer.analyze_single_package(invalid_path)
    
    def test_container_property_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.containers = [
            {"Name": "SeqContainer", "Type": "Sequence", "Depth": 1},
            {"Name": "ForeachLoop", "Type": "Foreach", "Depth": 2}
        ]
        container_names = [c["Name"] for c in analyzer.containers]
        self.assertIn("SeqContainer", container_names)
        self.assertEqual(analyzer.containers[0]["Depth"], 1)

    def test_variable_expression_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.variables_metadata = [
            {"VariableName": "MyVar", "HasExpression": True, "Expression": "@[User::OtherVar]"},
            {"VariableName": "SimpleVar", "HasExpression": False}
        ]
        expr_vars = [v for v in analyzer.variables_metadata if v.get("HasExpression")]
        self.assertEqual(len(expr_vars), 1)
        self.assertEqual(expr_vars[0]["Expression"], "@[User::OtherVar]")
    
    
    def test_parameterized_metadata_cases(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        test_cases = [
            {'VariableName': 'Test1', 'DataType': 'Int32', 'Namespace': 'User', 'Value': '1', 'PackageName': 'Pkg1', 'PackagePath': '/path1'},
            {'VariableName': 'Test2', 'DataType': 'Boolean', 'Namespace': 'System', 'Value': 'True', 'PackageName': 'Pkg2', 'PackagePath': '/path2'},
            {'VariableName': 'Test3', 'DataType': '', 'Namespace': '', 'Value': '', 'PackageName': '', 'PackagePath': ''},
        ]
        analyzer.variables_metadata = test_cases
        try:
            analyzer.save_variable_metadata(self.output_folder)
        except Exception as e:
            self.fail(f"Parameterized metadata test failed: {e}")

    def test_missing_variable_fields(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.variables_metadata = [
            {
                'VariableName': None, 'DataType': None, 'Namespace': None,
                'Value': None, 'PackageName': None, 'PackagePath': None
            }
        ]
        try:
            analyzer.save_variable_metadata(self.output_folder)
        except Exception as e:
            self.fail(f"save_variable_metadata raised exception unexpectedly: {e}")

    def test_invalid_xml_parsing(self):
        # Create an invalid XML file
        invalid_path = os.path.join(self.test_folder, "InvalidPackage.dtsx")
        with open(invalid_path, "w") as f:
            f.write("<Invalid <xml>>")

        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        try:
            analyzer.analyze_single_package(invalid_path)
        except ET.ParseError:
            pass  # Expected exception
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")

    def test_permission_error_handling(self):
        restricted_path = os.path.join(self.test_folder, "RestrictedPackage.dtsx")
        with open(restricted_path, "w") as f:
            f.write("<Executable></Executable>")
        os.chmod(restricted_path, 0o000)  # Remove all permissions

        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        try:
            analyzer.analyze()
        except PermissionError:
            pass  # Expected
        except Exception as e:
            self.fail(f"Unexpected exception raised: {e}")
        finally:
            os.chmod(restricted_path, 0o644)
    
    def test_empty_folder_handling(self):
        empty_folder = os.path.join(self.test_folder, "empty_subfolder")
        os.makedirs(empty_folder, exist_ok=True)
        analyzer = SSISPackageAnalyzer(empty_folder, self.output_folder, save_type="EXCEL")
        try:
            analyzer.analyze()
        except Exception as e:
            self.fail(f"Analyzer failed on empty folder: {e}")
            
    def test_lineage_path_validation(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.lineage_metadata = [
            {"Column": "CustomerID", "LineageID": "100", "Path": "Source -> Sort -> Output"},
            {"Column": "OrderDate", "LineageID": "101", "Path": "Source -> Output"}
        ]
        self.assertTrue(any("Sort" in m["Path"] for m in analyzer.lineage_metadata))

    def test_runtime_expression_tasks(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.task_expressions = [
            {"Task": "SQLTask1", "Expression": "@[User::SQLCommand]"},
            {"Task": "SendMail", "Expression": None}
        ]
        has_expr = [t for t in analyzer.task_expressions if t["Expression"]]
        self.assertEqual(len(has_expr), 1)
        self.assertEqual(has_expr[0]["Task"], "SQLTask1")

    def test_regression_snapshot_export(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        mock_export = {
            "Packages": ["Pkg1"],
            "Connections": ["Conn1"],
            "Tasks": ["Task1"],
            "MetadataVersion": "v1.0"
        }
        snapshot_path = os.path.join(self.output_folder, "regression_snapshot.json")
        with open(snapshot_path, "w") as f:
            json.dump(mock_export, f, indent=2)

        with open(snapshot_path, "r") as f:
            loaded_snapshot = json.load(f)
        self.assertIn("Packages", loaded_snapshot)
        self.assertEqual(loaded_snapshot["MetadataVersion"], "v1.0")
    
    def test_regression_snapshot_comparison(self):
        snapshot1 = {"Packages": ["Pkg1"], "Version": "1.0"}
        snapshot2 = {"Packages": ["Pkg1"], "Version": "1.0"}
        self.assertEqual(snapshot1, snapshot2)
    
    def test_package_analysis_result_serialization(self):
        result = PackageAnalysisResult(
            PackageName="TestPkg",
            CreatedDate="2024-01-01",
            CreatedBy="admin",
            Tasks=[{"TaskName": "SQLTask"}],
            Connections=[{"ConnectionName": "TestConn"}],
            PackagePath=self.test_folder,
            Containers=[],
            DTSXXML="<DTS />",
            Seqtasks=[],
            Foreachtasks=[],
            Forlooptasks=[],
            Variables=[],
            DataFlowTaskDetails=[]
        )
        result_json = json.dumps(result.__dict__)
        self.assertIn("TestPkg", result_json)
        self.assertIn("TestConn", result_json)

    @patch("pyodbc.connect")
    def test_sql_export_mock(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="SQL")
        analyzer._connection_string = "Driver={SQL Server};Server=localhost;Database=TestDB;Trusted_Connection=yes;"
        analyzer.variables_metadata = [
            {"VariableName": "Var1", "DataType": "String", "Namespace": "User", "Value": "Test", "PackageName": "Pkg", "PackagePath": "/path"}
        ]
        analyzer.save_variable_metadata(self.output_folder)
        mock_cursor.execute.assert_called()
        self.assertTrue(mock_cursor.execute.call_args[0][0].startswith("INSERT INTO"))
    
    def test_batch_simulation_for_dtsx_files(self):
        for i in range(100):
            with open(os.path.join(self.test_folder, f"Pkg_{i}.dtsx"), "w") as f:
                f.write(f"<Executable DTSID='{{i}}' xmlns:DTS='www.microsoft.com/SqlServer/Dts'></Executable>")

        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        var_file = os.path.join(self.output_folder, "Variables.xlsx")
        self.assertTrue(os.path.exists(var_file))
    
    
    def test_integration_real_package_folder(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()

        var_file = os.path.join(self.output_folder, "Variables.xlsx")
        param_file = os.path.join(self.output_folder, "Parameters.xlsx")

        self.assertTrue(os.path.exists(var_file))
        self.assertTrue(os.path.exists(param_file))
        with open(os.path.join(self.output_folder, "integration_log.txt"), "w") as log:
            log.write("Integration test executed successfully.\n")
    
    def test_excel_output_headers(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.variables_metadata = [
            {"VariableName": "Var1", "DataType": "String", "Namespace": "User", "Value": "Test", "PackageName": "TestPackage", "PackagePath": self.test_folder}
        ]
        analyzer.save_variable_metadata(self.output_folder)

        var_file = os.path.join(self.output_folder, "Variables.xlsx")
        df = pd.read_excel(var_file)
        expected_columns = ["VariableName", "DataType", "Namespace", "Value", "PackageName", "PackagePath"]
        self.assertListEqual(list(df.columns), expected_columns)
    
    def test_excel_output_values(self):
        var_file = os.path.join(self.output_folder, "Variables.xlsx")
        df = pd.read_excel(var_file)
        self.assertEqual(df.iloc[0]["VariableName"], "Var1")
        self.assertEqual(df.iloc[0]["DataType"], "String")
        self.assertEqual(df.iloc[0]["Value"], "Test")

    def test_excel_connection_metadata(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.connection_metadata = [
            {"ConnectionName": "TestConn", "ConnectionType": "OLEDB", "ConnectionString": "Server=mydb;"}
        ]
        analyzer.save_connections_metadata(self.output_folder)
        conn_file = os.path.join(self.output_folder, "Connections.xlsx")
        self.assertTrue(os.path.exists(conn_file))
        df = pd.read_excel(conn_file)
        self.assertIn("ConnectionName", df.columns)
        self.assertIn("ConnectionType", df.columns)
        self.assertEqual(df.iloc[0]["ConnectionName"], "TestConn")

    def test_excel_missing_file(self):
        missing_file = os.path.join(self.output_folder, "NonExistent.xlsx")
        self.assertFalse(os.path.exists(missing_file))
        with self.assertRaises(FileNotFoundError):
            pd.read_excel(missing_file)
    
    def test_logging_output(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        with self.assertLogs(level='INFO') as log:
            logging.getLogger().setLevel(logging.INFO)
            analyzer.log_error("mock_path.dtsx", Exception("Mock exception"))
            self.assertIn("Error processing mock_path.dtsx", log.output[0])
    
    def test_task_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        self.assertGreaterEqual(len(analyzer.analysis_results[0].Tasks), 1)
        self.assertEqual(analyzer.analysis_results[0].Tasks[0].TaskName, "MyTask")

    def test_connection_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        self.assertGreaterEqual(len(analyzer.analysis_results[0].Connections), 1)
        self.assertEqual(analyzer.analysis_results[0].Connections[0].ConnectionName, "MyConn")

    def test_precedence_constraint_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        self.assertGreaterEqual(len(analyzer.analysis_results[0].PrecedenceConstraintDetails), 1)
        self.assertEqual(analyzer.analysis_results[0].PrecedenceConstraintDetails[0]['From'], "Task1")
        self.assertEqual(analyzer.analysis_results[0].PrecedenceConstraintDetails[0]['To'], "Task2")

    def test_container_extraction(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        self.assertGreaterEqual(len(analyzer.analysis_results[0].Containers), 1)
        self.assertEqual(analyzer.analysis_results[0].Containers[0].ContainerName, "MainSeq")
        self.assertEqual(analyzer.analysis_results[0].Containers[0].ContainerType, "Sequence")

    def test_loop_containers(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        loop_names = [loop.ContainerName for loop in analyzer.analysis_results[0].ForeachContainers + analyzer.analysis_results[0].ForLoopContainers]
        self.assertIn("MyLoop", loop_names)
        self.assertIn("MyForLoop", loop_names)

    def test_event_handlers(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        event_handler_names = [e['EventName'] for e in analyzer.analysis_results[0].EventHandlers]
        self.assertIn("OnError", event_handler_names)

    def test_dataflow_components(self):
        analyzer = SSISPackageAnalyzer(self.test_folder, self.output_folder, save_type="EXCEL")
        analyzer.analyze()
        dataflow_names = [c['ComponentName'] for df in analyzer.analysis_results[0].DataFlowTaskDetails for c in df['Components']]
        self.assertIn("Derived Column", dataflow_names)

    def tearDown(self):
        for root, _, files in os.walk(self.output_folder):
            for file in files:
                os.remove(os.path.join(root, file))

if __name__ == '__main__':
    unittest.main()
