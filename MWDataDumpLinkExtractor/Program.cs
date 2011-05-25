using System;
using System.IO;
using MySql.Data;
using Utility;

namespace MWDataDumpLinkExtractor
{
    class Program
    {
        private static DatabaseLayer dal;

        static void Main(string[] args)
        {
            if (!new FileInfo("dbconfig.txt").Exists)
            {
                DotHmbotConfigurationFile.Create("dbconfig.txt", "host", 3306, "username", "password", "schema");

                return;   
            }

            DotHmbotConfigurationFile dotHmbotConfigurationFile = new DotHmbotConfigurationFile("dbconfig.txt");

            dal = new DatabaseLayer(
                dotHmbotConfigurationFile.mySqlServerHostname,
                dotHmbotConfigurationFile.mySqlServerPort,
                dotHmbotConfigurationFile.mySqlUsername,
                dotHmbotConfigurationFile.mySqlPassword,
                dotHmbotConfigurationFile.mySqlSchema,
                1000
                );

            dal.NextSetReadyEvent += firstSetReadyEventHandler;

            dal.initialise();
        }

        static void firstSetReadyEventHandler(object sender, TokenEventArgs e)
        {
            dal.NextSetReadyEvent -= firstSetReadyEventHandler;


        }
    }
}