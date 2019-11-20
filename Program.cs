using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace MigradorDatos
{
    class Program
    {
        private static string SOURCE_CONN_STRING = "";
        private static string TARGET_CONN_STRING = "";
        static void Main(string[] args)
        {
            string sourceCommand;
            string targetTable;
            Dictionary<string, string> targetColumns;

            sourceCommand = @"SELECT Accounting_Week_Num, Company_Code, End_Date, Month_Code, Start_Date, Week_Count, Dt_Generation_Id,
            Dt_Transaction_Id, Dt_Write_Bln, Last_Updated_Datetime, Last_Updated_Id, Last_Updated_Timestamp FROM [Accounting Week]";
            targetTable = "AccountingWeek";
            targetColumns = new Dictionary<string, string>()
            {
                { "Accounting_Week_Num",    "System.Int32"  },
                { "Company_Code",           "System.String" },
                { "End_Date",               "System.String" },
                { "Month_Code" ,            "System.String" },
                { "Start_Date",             "System.String" },
                { "Week_Count",             "System.Int32"  },
                { "Dt_Generation_Id",       "System.Int32"  },
                { "Dt_Transaction_Id" ,     "System.Guid"   },
                { "Dt_Write_Bln",           "System.Boolean"},
                { "Last_Updated_Datetime",  "System.String" },
                { "Last_Updated_Id",        "System.String" },
                { "Last_Updated_Timestamp", "System.String" }
            };
            //BulkInsertFrom(sourceCommand, targetTable, targetColumns, 1000, true);

            sourceCommand =  @"SELECT Agency_Id, Accounting_Week_Num, Maintained_By_Db_Id, Agency_Status_Type_Code FROM Agency_Table";
            targetTable   = "AgencyTable";
            targetColumns = new Dictionary<string, string>()
            {
                { "Agency_Id",               "System.Int64" },
                { "Accounting_Week_Num",      "System.Int32" },
                { "Maintained_By_Db_Id",     "System.Int16" },
                { "Agency_Status_Type_Code", "System.String"}
            };
            //BulkInsertFrom(sourceCommand, targetTable, targetColumns, 1000, true);

            sourceCommand = @"SELECT Agreement_Id, Agreement_Db_Id, Absolute_Arrears, Accounting_Week_Num, Company_Code, Expected_Payment, Total_Amount_Payable,
            Regular_Instalment_Value, Active_Rate, Outstanding_Balance FROM Agreement_Historic_Data_Table";
            targetTable   = "AgreementHistoricDataTable";
            targetColumns = new Dictionary<string, string>()
            {
                { "Agreement_Id",               "System.Int32" },
                { "Agreement_Db_Id",            "System.Int16"},
                { "Absolute_Arrears",           "System.Double"},
                { "Accounting_Week_Num",        "System.Int32"},
                { "Company_Code",               "System.String"},
                { "Expected_Payment",           "System.Double"},
                { "Total_Amount_Payable",       "System.Double"},
                { "Regular_Instalment_Value",   "System.Double"},
                { "Active_Rate",                "System.Double"},
                { "Outstanding_Balance",        "System.Double"}
            };
            //BulkInsertFrom(sourceCommand, targetTable, targetColumns, 2500, true);

            sourceCommand = @"SELECT Customer_Id, Agreement_Id, Agreement_Db_Id, Agreement_Num, Creation_Week, Cancellation_Week, Actual_Paid_Up_Week, Written_Off_Week, 
            Written_Off_Value, Written_Off_Date, Cancellation_Date, Creation_Date, Actual_Paid_Up_Date, Instalment_Periods_To_Date, Product_Version_Id, Agreement_Status_Code, 
            Customer_Type_Code_At_Issue FROM Agreement_Table";
            targetTable = "AgreementTable";
            targetColumns = new Dictionary<string, string>()
            {
                { "Customer_Id",                    "System.Int64" },
                { "Agreement_Id",                   "System.Int32" },
                { "Agreement_Db_Id",                "System.Int16"},
                { "Agreement_Num",                  "System.Int64" },
                { "Creation_Week",                  "System.Int32" },
                { "Cancellation_Week",              "System.Int32"},
                { "Actual_Paid_Up_Week",            "System.Int32"},
                { "Written_Off_Week",               "System.Int32"},
                { "Written_Off_Value",              "System.Double"},
                { "Written_Off_Date",               "System.String"},
                { "Cancellation_Date",              "System.String"},
                { "Creation_Date",                  "System.String"},
                { "Actual_Paid_Up_Date",            "System.String"},
                { "Instalment_Periods_To_Date",     "System.Int16"},
                { "Product_Version_Id",             "System.Int32"},
                { "Agreement_Status_Code",          "System.String"},
                { "Customer_Type_Code_At_Issue",    "System.String"}
            };
            //BulkInsertFrom(sourceCommand, targetTable, targetColumns, 1000, true);

            sourceCommand = @"SELECT Agreement_Id, Agreement_Db_Id, Committed_Bln, Balanced_Bln, Accounting_Week_Num, Value, Agreement_Transaction_Type_Code, 
            Committed_Date FROM Agreement_Transaction_Table";
            targetTable   = "AgreementTransactionTable";
            targetColumns = new Dictionary<string, string>()
            {
                { "Agreement_Id",                    "System.Int32" },
                { "Agreement_Db_Id",                 "System.Int16"},
                { "Committed_Bln",                   "System.Boolean"},
                { "Balanced_Bln",                    "System.Boolean"},
                { "Accounting_Week_Num",             "System.Int32"},
                { "Value",                           "System.Double"},
                { "Agreement_Transaction_Type_Code", "System.String"},
                { "Committed_Date",                  "System.String"}
            };
            //BulkInsertFrom(sourceCommand, targetTable, targetColumns, 1000, true);

            sourceCommand = @"SELECT Customer_Id, Agency_Id, Accounting_Week_Num, Num_Of_Consecutive_Misses, Customer_Status_Code, Collection_Pct, Paid_Up_Week, 
            Written_OFF_Week, Loan_Arrears_Stage FROM Customer_Agency_History_Table";
            targetTable = "CustomerAgencyHistoryTable";
            targetColumns = new Dictionary<string, string>()
            {
                { "Customer_Id",                "System.Int64"},
                { "Agency_Id",                  "System.Int64"},
                { "Accounting_Week_Num",        "System.Int32"},
                { "Num_Of_Consecutive_Misses",  "System.Int16"},
                { "Customer_Status_Code",       "System.String"},
                { "Collection_Pct",             "System.Double"},
                { "Paid_Up_Week",               "System.Int32"},
                { "Written_OFF_Week",           "System.Int32"},
                { "Loan_Arrears_Stage",         "System.Int16"}
            };
            //BulkInsertFrom(sourceCommand, targetTable, targetColumns, 1000, true);

            sourceCommand = @"SELECT Customer_Transfer_Request_Id, Customer_Transfer_Request_Db_Id, Sending_Agency_Id, Receiving_Agency_Id, Week_The_Transfer_Was_Completed, 
            Sending_Role_Db_Id, Receiving_Role_Db_Id, Customer_Id, Week_Transfer_Requested, Status FROM Customer_Transfer_Request_Table";
            targetTable   = "CustomerTransferRequestTable";
            targetColumns = new Dictionary<string, string>()
            {
                { "Customer_Transfer_Request_Id",       "System.Int32" },
                { "Customer_Transfer_Request_Db_Id",    "System.Int16"},
                { "Sending_Agency_Id",                  "System.Int64"},
                { "Receiving_Agency_Id",                "System.Int64"},
                { "Week_The_Transfer_Was_Completed",    "System.Int32"},
                { "Sending_Role_Db_Id",                 "System.Int16"},
                { "Receiving_Role_Db_Id",               "System.Int16"},
                { "Customer_Id",                        "System.Int64"},
                { "Week_Transfer_Requested",            "System.Int32"},
                { "Status",                             "System.String"}
            };
            //BulkInsertFrom(sourceCommand, targetTable, targetColumns, 1000, true);


        }

        private static void BulkInsertFrom(string sourceCommandText, string targetTableName, Dictionary<string, string> targetTableColumns, int batchSize = 1000, bool clearTarget = false)
        {
            using(SqlConnection sourceConn = new SqlConnection(SOURCE_CONN_STRING))
            using(SqlConnection targetConn = new SqlConnection(TARGET_CONN_STRING))
            {
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();

                Console.WriteLine($"\nVolcando datos en tabla destino {targetTableName}\n");
                Console.WriteLine("Abriendo conexion con origen...");
                sourceConn.Open();
                var sourceCommand = sourceConn.CreateCommand();
                sourceCommand.CommandTimeout = 0;
                sourceCommand.CommandText = sourceCommandText;

                //  Creamos tabla de insercion masiva
                Console.WriteLine("Creando tabla de inserción...");
                DataTable bulkInsertTable = new DataTable();
                foreach(var column in targetTableColumns)
                {
                    bulkInsertTable.Columns.Add(new DataColumn(column.Key, Type.GetType(column.Value)));
                }

                Console.WriteLine("Ejecutando consulta en origen...");
                var sourceReader = sourceCommand.ExecuteReader();

                while(sourceReader.Read())
                {
                    DataRow row =  bulkInsertTable.NewRow();
                    foreach(var column in targetTableColumns)
                    {
                        row[column.Key] = sourceReader[column.Key];
                    }
                    bulkInsertTable.Rows.Add(row);
                }

                Console.WriteLine($"Total de registros en Origen: {bulkInsertTable.Rows.Count}");
                Console.WriteLine("Abriendo conexión en destino...");
                targetConn.Open();

                if(clearTarget)
                {
                    Console.WriteLine("Eliminando datos en tabla destino... inserción limpia");
                    var command = targetConn.CreateCommand();
                    command.CommandTimeout = 0;
                    command.CommandText = $"DELETE FROM {targetTableName}";
                    command.ExecuteNonQuery();
                }
               
                var targetTransaction = targetConn.BeginTransaction();

                using(var bulkInsert = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.Default, targetTransaction))
                {
                    bulkInsert.BulkCopyTimeout = 0;
                    bulkInsert.BatchSize = batchSize;
                    bulkInsert.DestinationTableName = targetTableName;
                    Console.WriteLine($"Insertando en batch de {bulkInsert.BatchSize}");
                    try
                    {
                        bulkInsert.WriteToServer(bulkInsertTable);
                        Console.WriteLine("Guardado en destino exitoso!");
                        targetTransaction.Commit();
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine("Error en insercion a destino.", e);
                        targetTransaction.Rollback();
                    }
                }
                
                Console.WriteLine("Cerrando conexiones...");
                stopWatch.Stop();
                Console.WriteLine($"Tiempo de ejecución: {stopWatch.Elapsed}");
                sourceConn.Close();
                targetConn.Close();
                Console.WriteLine("\n");
            }
        }
    }
}
