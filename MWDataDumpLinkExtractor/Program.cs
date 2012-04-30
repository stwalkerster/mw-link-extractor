using System;
using System.IO;
using MySql.Data.MySqlClient;

namespace TableSplitter
{
    class Program
    {
        static void Main(string[] args)
        {
            if (!new FileInfo("dbconfig.xml").Exists)
            {
                DotHmbotConfigurationFile.Create("dbconfig.xml", "host", 3306, "username", "password", "schema");

                return;
            }

            DotHmbotConfigurationFile dotHmbotConfigurationFile = DotHmbotConfigurationFile.Open("dbconfig.xml");

            MySqlConnection connection = new MySqlConnection(
                new MySqlConnectionStringBuilder()
                {
                    UserID = dotHmbotConfigurationFile[DotHmbotConfigurationFile.MYSQL_USERNAME],
                    Password = dotHmbotConfigurationFile[DotHmbotConfigurationFile.MYSQL_PASSWORD],
                    Server = dotHmbotConfigurationFile[DotHmbotConfigurationFile.MYSQL_SERVER],
                    Database = dotHmbotConfigurationFile[DotHmbotConfigurationFile.MYSQL_SCHEMA]
                }.ToString());

            connection.Open();


            while(true)
            {
                // start transaction
                MySqlTransaction t = connection.BeginTransaction(System.Data.IsolationLevel.RepeatableRead);

                // select 1 (or more) from externallinks FOR UPDATE
                MySqlCommand cmd = new MySqlCommand("SELECT * FROM externallinks LIMIT 1 FOR UPDATE;", connection, t);
                MySqlDataReader r = cmd.ExecuteReader();

                if (!r.Read()) break;

                int page = r.GetInt32(0);
                string url = r.GetString(1);
                string idx = r.GetString(2);

                r.Close();

                // insert into processed
                cmd = new MySqlCommand("INSERT INTO processed VALUES( @1, @2, @3);", connection, t);
                cmd.Parameters.AddWithValue("@1", page);
                cmd.Parameters.AddWithValue("@2", url);
                cmd.Parameters.AddWithValue("@3", idx);
                cmd.ExecuteNonQuery();

                // delete it from externallinks
                cmd = new MySqlCommand("DELETE FROM externallinks WHERE el_from = @1 AND el_to = @2 AND el_index = @3;", connection, t);
                cmd.Parameters.AddWithValue("@1", page);
                cmd.Parameters.AddWithValue("@2", url);
                cmd.Parameters.AddWithValue("@3", idx);
                //cmd.ExecuteNonQuery();

                Console.WriteLine(url);

                // (split data)
                Uri u = new Uri(url);
                string protocol = u.Scheme,
                    domain = u.DnsSafeHost;

                // insert ignore into protocol
                cmd = new MySqlCommand("INSERT IGNORE INTO protocol (protocol) VALUES (@1) ;", connection, t);
                cmd.Parameters.AddWithValue("@1", protocol);
                cmd.ExecuteNonQuery();

                // get protocol id
                cmd = new MySqlCommand("SELECT id FROM protocol WHERE protocol = @1 ;", connection, t);
                cmd.Parameters.AddWithValue("@1", protocol);
                int protocolid = (int)cmd.ExecuteScalar();

                // insert ignore into domain
                cmd = new MySqlCommand("INSERT IGNORE INTO domain (domain) VALUES (@1) ;", connection, t);
                cmd.Parameters.AddWithValue("@1", domain);
                cmd.ExecuteNonQuery();

                // get domain id
                cmd = new MySqlCommand("SELECT id FROM domain WHERE domain = @1 ;", connection, t);
                cmd.Parameters.AddWithValue("@1", domain);
                int domainid = (int)cmd.ExecuteScalar();

                // insert into link protocol domain link fragment page
                cmd = new MySqlCommand("INSERT INTO link (protocol, domain, path, fragment, page) VALUES( @1, @2, @3, @4, @5);", connection, t);
                cmd.Parameters.AddWithValue("@5", page);
                cmd.Parameters.AddWithValue("@2", domainid);
                cmd.Parameters.AddWithValue("@1", protocolid);
                cmd.Parameters.AddWithValue("@3", u.PathAndQuery);
                cmd.Parameters.AddWithValue("@4", u.Fragment);
                cmd.ExecuteNonQuery();

                // commit
                t.Commit();
                
            }
        }
    }
}