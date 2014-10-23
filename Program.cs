using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Configuration;
namespace OdbcImport
{
	static class Program
	{
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main()
		{
            string odbccon = ConfigurationSettings.AppSettings["odbcconnection"];
            var oc = new OdbcConnector(odbccon);


			oc.Load();

			Console.ReadLine();
			//Application.EnableVisualStyles();
			//Application.SetCompatibleTextRenderingDefault(false);
			//Application.Run(new Form1());
		}
	}
}
