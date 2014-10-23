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
				"users 2",
				"wage"              ,
                "SUTxASG"           ,
                "groupaccount"      ,
                "T20ORT"            ,
                "T20ORTsil"         ,
                "coefficientCity"   ,
                "coefficients"      ,
                "coefficientsLook"  ,
                "T34LAB"            ,
                "T34LABsil"         ,
                "asgSectionSelector",
                "asgSpec"           ,
                "T14KVC"            ,
                "T14KVCsil"         ,
                "T04COC"            ,
                "T04COCsil"         ,
                "T24RAD"            ,
                "T24RADsil"         ,
                "T18NRS"            ,
                "T18NRSsil"         ,
                "T21PLS"            ,
                "T21PLSsil"         ,
                "T08GNC"            ,
                "T08GNCsil"         ,
                "ABkzLink"          ,
                "asgLink"           ,
                "T03ANE"            ,
                "T03ANEsil"         ,
                "T13KHD"            ,
                "T13KHDsil"         ,
                "sectionChooser"    ,
                "sectionPageBreak"  ,
                "T16KBB"            ,
                "T16KBBsil"         ,
                "ASGxASG"           ,
                "BkzLink"           ,
                "T26URO"            ,
                "T26UROsil"         ,
                "T11GOZ"            ,
                "T11GOZsil"         ,
                "users"             ,
                "T28PAT"            ,
                "T28PATsil"         ,
                "queRow"            ,
                "T09GOC"            ,
                "T09GOCsil"         ,
                "T12ICH"            ,
                "T12ICHsil"         ,
                "T19NTP"            ,
                "T19NTPsil"         ,
                "queASG"            ,
                "T02ACT"            ,
                "T02ACTsil"         ,
                "ansRow"            ,
                "T06DER"            ,
                "T06DERsil"         ,
                "T07FTR"            ,
                "T07FTRsil"         ,
                "T17NOR"            ,
                "T17NORsil"         ,
                "T15KAR"            ,
                "T15KARsil"         ,
                "T27RON"            ,
                "T27RONsil"         ,
                "T30TGN"            ,
                "T30TGNsil"         ,
                "T10GOH"            ,
                "T10GOHsil"         ,
                "T22PSK"            ,
                "T22PSKsil"         ,
                "T05CSH"            ,
                "T05CSHsil"         ,
                "reviews"           ,
                "T31END"            ,
                "T31ENDsil"         ,
                "T33DOK"            ,
                "T33DOKsil"         ,
                "T00ILKsil"         ,
                "T01MUA"            ,
                "T01MUAsil"         ,
                "T32SPO"            ,
                "T32SPOsil"         ,
                "ansASG"            ,
                "T25SHT"            ,
                "T25SHTsil"         ,
                "T23COP"            ,
                "T23COPsil"         ,
                "globalASG"         ,
                "globalSUT"         ,
                "globaROW"          ,
                "globalANT"         ,
                "mails"             ,
                "T29UYA"            ,
                "T29UYAsil"         ,
                "userMails",
                "row00ILK",
                "row01MUA",
                "row02ACT",
                "row03ANE",
                "row04COC",
                "row05CSH",
                "row06DER",
                "row07FTR",
                "row08GNC",
                "row09GOC",
                "row10GOH",
                "row11GOZ",
                "row12ICH",
                "row13KHD",
                "row14KVC",
                "row15KAR",
                "row16KBB",
                "row17NOR",
                "row18NRS",
                "row19NTP",
                "row20ORT",
                "row21PLS",
                "row22PSK",
                "row23COP",
                "row24RAD",
                "row25SHT",
                "row26URO",
                "row27RON",
                "row28PAT",
                "row29UYA",
                "row30TGN",
                "row31END",
                "row32SPO",
                "row33DOK",
                "row34LAB",
                "rowChecksumDup",
                "rowDup",
                "rowSearch",
                "Siblings",
                "section",
                "SiblingsOrder",
                "SUT",
                "SUTParent",                
                "sut9",
                "SUTDescendants",
                "SUTOrphans",
                "SUTParent",
                "SUTSiblings",
                "SUTx",
                //tablosayısı 0 olanlar
                "meshaBak",
                "meshANAsearch",
                "meshANTup",
                "MESHaxASG",
                "meshiBak",
                "MESHixASG",
                "meshOPRsearch",
                "meshOPRup",
                "MESHuxASG",
                "meshUZMsearch",
                "meshUZMup"
                
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
