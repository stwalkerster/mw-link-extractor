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

                int id = r.GetInt32(0);
                int page = r.GetInt32(1);
                string url = r.GetString(2);

                r.Close();

                // insert into processed
                cmd = new MySqlCommand("INSERT INTO processed (id, el_from, el_to) VALUES( @1, @2, @3);", connection, t);
                cmd.Parameters.AddWithValue("@1", id);
                cmd.Parameters.AddWithValue("@2", page);
                cmd.Parameters.AddWithValue("@3", url);
                cmd.ExecuteNonQuery();

                // delete it from externallinks
                cmd = new MySqlCommand("DELETE FROM externallinks WHERE id = @1;", connection, t);
                cmd.Parameters.AddWithValue("@1", id);
                cmd.ExecuteNonQuery();

                Console.Write(url);

                // commit
                t.Commit();
                

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

                Console.WriteLine(" ... DONE");
            }
        }
    }
}