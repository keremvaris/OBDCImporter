using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
namespace OdbcImport
{
    public class OdbcConnector
    {
        private string CDN;
        public OdbcConnector(string cdn)
        {
            CDN = cdn;
        }
        private static Hashtable dbTypeTable;

        private string ConvertToDbType(Type t)
        {
            if (dbTypeTable == null)
            {
                dbTypeTable = new Hashtable();
                dbTypeTable.Add(typeof(System.Boolean), SqlDbType.Bit);
                dbTypeTable.Add(typeof(System.Int16), SqlDbType.SmallInt);
                dbTypeTable.Add(typeof(System.Int32), SqlDbType.Int);
                dbTypeTable.Add(typeof(System.Int64), SqlDbType.BigInt);
                dbTypeTable.Add(typeof(System.Double), SqlDbType.Float);
                dbTypeTable.Add(typeof(System.Decimal), SqlDbType.Decimal);
                dbTypeTable.Add(typeof(System.String), SqlDbType.VarChar);
                dbTypeTable.Add(typeof(System.DateTime), SqlDbType.DateTime);
                dbTypeTable.Add(typeof(System.Byte[]), SqlDbType.VarBinary);
                dbTypeTable.Add(typeof(System.Guid), SqlDbType.UniqueIdentifier);
            }
            SqlDbType dbtype;
            try
            {                
                dbtype = (SqlDbType)dbTypeTable[t];               
            }
            catch
            {
                dbtype = SqlDbType.Variant;
            }
            
            switch (dbtype)
            {
                case SqlDbType.NVarChar:   
                case SqlDbType.VarBinary:                    
                case SqlDbType.VarChar:
                    return dbtype.ToString() + "(MAX)";                
                default:
                    break;
            }
            return dbtype.ToString();
        }
        public void createsqltable(DataTable dt, string tablename)
        {
            string strconnection = ConfigurationSettings.AppSettings["sqlconnection"].ToString();
            string table = "";
            table += "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[" + tablename + "]') AND type in (N'U'))";
            table += "BEGIN ";
            table += "create table " + tablename + "";
            table += "(";
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                if (i != dt.Columns.Count - 1)
                    table += "[" + dt.Columns[i].ColumnName.Replace("key", "_key").Replace("Index", "_Index").Replace("user", "_user_").Replace("index", "_Index") + "]" + " " + ConvertToDbType(dt.Columns[i].DataType) + ",";
                else
                    table += "[" + dt.Columns[i].ColumnName.Replace("key", "_key").Replace("Index", "_Index").Replace("user", "_user_").Replace("index", "_Index") + "]" + " " + ConvertToDbType(dt.Columns[i].DataType);
            }
            table += ") ";
            table += "END";
            InsertQuery(table, strconnection);
            CopyData(strconnection, dt, tablename);
        }
        public void InsertQuery(string qry, string connection)
        {
            SqlConnection _connection = new SqlConnection(connection);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = qry;
            cmd.Connection = _connection;
            _connection.Open();
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {

                Console.Write(cmd.CommandText);
            }
            
            _connection.Close();
        }
        public static void CopyData(string connStr, DataTable dt, string tablename)
        {
            using (SqlBulkCopy bulkCopy =
            new SqlBulkCopy(connStr, SqlBulkCopyOptions.TableLock))
            {
                bulkCopy.DestinationTableName = tablename;
                bulkCopy.WriteToServer(dt);
            }
        }


        public void Load()
        {
            var exclude = new HashSet<string>()
			{
				"user",
				"row"              ,
              			//etc reserved sql keywords
                
			};
            string path = @"D:\HUV\";
            string sql = @"SELECT  * FROM  ";
            DataTable tables;
            using (var con = new OdbcConnection(CDN))
            {
                con.Open();

                tables = con.GetSchema("Tables");
                foreach (DataRow rw in tables.Rows)
                {
                    var tn = rw[2].ToString();
                    var objType = rw[3].ToString();
                    if (objType == "TABLE") //&& !exclude.Contains(tn))
                    {
                        Console.WriteLine("Reading {0}", tn);

                        try
                        {
                            using (var cmd = con.CreateCommand())
                            {
                                cmd.CommandText = "SELECT COUNT(*) FROM " + tn;

                                Console.WriteLine("Count {0} {1}", tn, cmd.ExecuteScalar());
                            }

                            var dt = new DataTable(tn);
                            using (var da = new OdbcDataAdapter(sql + tn, con))
                            {

                                da.Fill(0,10,dt);
                                Console.WriteLine("Saving {0}", tn);
                                //dt.WriteXml(path + tn + ".xml");
                            }
                            createsqltable(dt, tn);


                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine("------------");
                            Console.WriteLine(ex.ToString());
                            Console.WriteLine("------------");
                        }



                    }
                    System.Threading.Thread.Sleep(10);
                }
                con.Close();
            }
        }
    }
}
