using System;
using System.CodeDom;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using Npgsql;

namespace CopyTable
{
  internal static class Program
  {
    static void Main()
    {
      Action<string> Display = Console.WriteLine;
      string connectionString = GetConnectionString();
      // disable first all triggers
      string sqlRequest = GetTriggerTodisableSqlRequest();
      var triggerList = GetTriggerListFromDatabase(sqlRequest, connectionString);
      bool disableTriggersOk = EnableOrDisableTriggers(connectionString, triggerList);
      if (!disableTriggersOk)
      {
        Display("Error while disabling triggers.");
        return;
      }

      // get prerequisite tables to be updated first
      var tableName = "table1";
      var tablesToUpdate = GetUpdateOrder(tableName, connectionString);
      if (tablesToUpdate.Count == 0)
      {
        Display("No tables to update.");

      }
      else
      {
        Display($"There is {tablesToUpdate.Count} tables to be updated:");
      }

      tablesToUpdate.Add(tableName);
      foreach (var table in tablesToUpdate)
      {
        Display(table);
      }

      // Copy tables in order
      foreach (var table in tablesToUpdate)
      {
        Display($"Copying table {table}...");
        CopyTable(table, connectionString);
      }

      // enable triggers
      Display("Enabling triggers...");
      sqlRequest = GetTriggerToEnableSqlRequest();
      triggerList = GetTriggerListFromDatabase(sqlRequest, connectionString);
      bool enableTriggersOk = EnableOrDisableTriggers(connectionString, triggerList);
      if (!enableTriggersOk)
      {
        Display("Error while enabling triggers.");
      }

      Display("Press any key to exit:");
      Console.ReadKey();
    }

    private static void CopyTable(string table, string connectionString)
    {
      // to be implemented
    }

    private static List<string> GetUpdateOrder(string tableName, string connectionString)
    {
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

    private static bool EnableOrDisableTriggers(string connectionString, List<string> triggerList)
    {
      bool result = true;
      try
      {
        using (var connection = new NpgsqlConnection(connectionString))
        {
          connection.Open();
          foreach (var trigger in triggerList)
          {
            using (var command = new NpgsqlCommand(trigger, connection))
            {
              command.ExecuteNonQuery();
            }
          }
        }
      }
      catch (Exception)
      {
        result = false;
      }

      return result;
    }

    private static string GetConnectionString()
    {
      string connectionStringFromFile = ReadFile("connectionString.txt");
      if (!string.IsNullOrEmpty(connectionStringFromFile))
      {
        return connectionStringFromFile;
      }

      return "Host=localhost;Username=username;Password=password;Database=database_name";
    }

    private static string ReadFile(string filename)
    {
      try
      {
        // read the file filename and return its content
        if (!File.Exists(filename))
        {
          File.Create(filename).Close();
          WriteToFile(filename, "Host=localhost;Username=username;Password=password;Database=database_name");
          return string.Empty;
        }

        return File.ReadAllText(filename);
      }
      catch (Exception)
      {
        return string.Empty;
      }
    }

    private static void WriteToFile(string filename, string message)
    {
      try
      {
        // write the message to the file filename
        File.WriteAllText(filename, message);
      }
      catch (Exception)
      {
        // ignored
      }
    }

    private static string GetTriggerTodisableSqlRequest()
    {
      const string sqlRequest = "SELECT 'ALTER TABLE ' || event_object_table || ' DISABLE TRIGGER ' || trigger_name || ';' from information_schema.triggers where trigger_schema = current_user;";
      return sqlRequest;
    }

    private static string GetTriggerToEnableSqlRequest()
    {
      const string sqlRequest = "SELECT 'ALTER TABLE ' || event_object_table || ' ENABLE TRIGGER ' || trigger_name || ';' from information_schema.triggers where trigger_schema = current_user;";
      return sqlRequest;
    }

    private static List<string> GetTriggerListFromDatabase(string sqlRequest, string connectionString)
    {
      var result = new List<string>();
      using (var connection = new NpgsqlConnection(connectionString))
      {
        connection.Open();
        using (var command = new NpgsqlCommand(sqlRequest, connection))
        {
          using (var reader = command.ExecuteReader())
          {
            while (reader.Read())
            {
              var triggerName = reader.GetString(0);
              Console.WriteLine($"Found trigger: {triggerName}");
              result.Add(triggerName);
            }
          }
        }
      }

      return result;
    }
  }
}
