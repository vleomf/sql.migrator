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
            Dictionary<string, System.Type> targetColumns;

            sourceCommand = @"SELECT
                             [COL_ORIG]             as ColAlias
                        FROM [dbo].[TABLE]";
            targetTable = "TARGET_TABLE";
            
            //  El alias debe coincidir con el nombre de columna de la base
            //  de datos TARGET
            targetColumns = new Dictionary<string, System.Type>()
            {
                //Columna TARGET            //Tipo de dato Columna TARGET
                { "ColAlias" ,              typeof(long)  },
            };
            BulkInsertFrom(sourceCommand, targetTable, targetColumns, 100, true);
        }

        private static void BulkInsertFrom(string sourceCommandText, string targetTableName, Dictionary<string, System.Type> targetTableColumns, int batchSize = 1000, bool clearTarget = false)
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
                Console.WriteLine("Creando tabla de inserción en memoria...");
                DataTable bulkInsertTable = new DataTable();
                foreach(var column in targetTableColumns)
                {
                    try
                    {
                        bulkInsertTable.Columns.Add(new DataColumn(column.Key, column.Value));
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine("\nError al crear tabla de inserción en memoria");
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"{e.Message}");
                        Console.WriteLine($"dataType enviado : {column.Value}");
                        Console.ForegroundColor = ConsoleColor.White;
                        Console.WriteLine($"Tiempo de ejecución: {stopWatch.Elapsed}");
                        return;
                    }
                    
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
                    
                    //  Mapeamos origen a destino
                    //  alias => columna
                    foreach(var column in targetTableColumns)
                    {
                        bulkInsert.ColumnMappings.Add(column.Key, column.Key);
                    }
                    
                    Console.WriteLine($"Insertando en batch de {bulkInsert.BatchSize}");
                    try
                    {
                        bulkInsert.WriteToServer(bulkInsertTable);
                        Console.WriteLine("Guardado en destino exitoso!");
                        targetTransaction.Commit();
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine("Error en inserción a destino!");
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"\n{e.Message}\n");
                        Console.ForegroundColor = ConsoleColor.White;
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
