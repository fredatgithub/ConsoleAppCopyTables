using System;
using System.Collections.Generic;
using Npgsql;

namespace ConsoleAppCopyTablesInOrder
{
  internal class Program
  {
    static void Main()
    {
      const string tableName = "activities";
      var tablesToUpdate = GetUpdateOrder(tableName);
      if (tablesToUpdate.Count == 0)
      {
        Console.WriteLine("No tables to update.");
      }
      else
      {
        Console.WriteLine($"There is {tablesToUpdate.Count} tables to be updated:");
      }

      foreach (var table in tablesToUpdate)
      {
        Console.WriteLine(table);
      }

      Console.WriteLine("Press any key to exit:");
      Console.ReadKey();
    }

    private static string GettriggerTodisableSqlRequest()
    {
      return "SELECT 'ALTER TABLE ' || event_object_table || ' DISABLE TRIGGER ' || trigger_name || ';' from information_schema.triggers where trigger_schema = user ORDER by event_object_table, trigger_name;";
    }

    private List<string> GetTriggerListFromDatabase(string request, string connectionString)
    {
      var result = new List<string>();
      using (var connection = new NpgsqlConnection(connectionString))
      {
        connection.Open();
        using (var command = new NpgsqlCommand(request, connection))
        {
          using (var reader = command.ExecuteReader())
          {
            while (reader.Read())
            {
              result.Add(reader.GetString(0));
            }
          }
        }
      }

      return result;
    }

    private static List<string> GetUpdateOrder(string tableName)
    {
      const string connectionString = "Host=localhost;Username=username;Password=password;Database=database_name";
      var tablesToUpdate = new List<string>();

      using (var connection = new NpgsqlConnection(connectionString))
      {
        connection.Open();

        string query = @"
                WITH RECURSIVE dependency_tree AS (
                    SELECT 
                        tc.table_name, 
                        ccu.table_name AS foreign_table_name
                    FROM 
                        information_schema.table_constraints AS tc
                    JOIN 
                        information_schema.key_column_usage AS kcu
                    ON 
                        tc.constraint_name = kcu.constraint_name
                    JOIN 
                        information_schema.constraint_column_usage AS ccu
                    ON 
                        ccu.constraint_name = tc.constraint_name
                    WHERE 
                        tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_name = @TableName
                    UNION
                    SELECT 
                        tc.table_name, 
                        ccu.table_name AS foreign_table_name
                    FROM 
                        information_schema.table_constraints AS tc
                    JOIN 
                        information_schema.key_column_usage AS kcu
                    ON 
                        tc.constraint_name = kcu.constraint_name
                    JOIN 
                        information_schema.constraint_column_usage AS ccu
                    ON 
                        ccu.constraint_name = tc.constraint_name
                    JOIN 
                        dependency_tree dt
                    ON 
                        dt.table_name = tc.table_name
                )
                SELECT 
                    DISTINCT table_name
                FROM 
                    dependency_tree
                WHERE 
                    table_name != @TableName
                ORDER BY 
                    table_name;
            ";

        using (var command = new NpgsqlCommand(query, connection))
        {
          command.Parameters.AddWithValue("@TableName", tableName);

          using (var reader = command.ExecuteReader())
          {
            while (reader.Read())
            {
              tablesToUpdate.Add(reader.GetString(0));
            }
          }
        }
      }

      return tablesToUpdate;
    }
  }
}
