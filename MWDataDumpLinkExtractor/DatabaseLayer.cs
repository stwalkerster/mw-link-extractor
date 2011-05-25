using System;
using System.Collections.Generic;
using System.Data;
using System.Security;
using System.Text;
using System.Threading;
using MySql.Data.MySqlClient;

namespace MWDataDumpLinkExtractor
{
    sealed class DatabaseLayer
    {
        private MySqlConnection _conn;

        private object padlock = new object();

        private Dictionary<int, string> nextSet;
        private string setToken = "EMPTY";
        private int _setSize = 1000;
        private int _nextSet = 0;

        private MySqlCommand cmd;


        private Thread retrieverThread;

        public DatabaseLayer(string host, uint port, string username, string password, string schema, int setSize)
        {
            _setSize = setSize;
            MySqlConnectionStringBuilder mcsb = new MySqlConnectionStringBuilder
                                                    {
                                                        Server = host,
                                                        Port = port,
                                                        UserID = username,
                                                        Password = password,
                                                        Database = schema
                                                    };

            _conn = new MySqlConnection(mcsb.GetConnectionString(true));
            retrieverThread = new Thread(getNextSetWorker);
        }

        private void getNextSetWorker()
        {
            lock (padlock)
            {
                

                MySqlParameter start = cmd.Parameters.Add("?start", MySqlDbType.UInt32);
                MySqlParameter limit = cmd.Parameters.Add("?limit", MySqlDbType.UInt32);

                start.Value = _nextSet;
                limit.Value = _setSize;

                DataSet ds = new DataSet();
                MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                da.Fill(ds);

                DataTableReader dataTableReader = ds.Tables[0].CreateDataReader();

                nextSet = new Dictionary<int, string>(_setSize);
                do
                {
                    object[] vals = new object[5]; // from, to, id, status, timestamp
                    dataTableReader.GetValues(vals);


                } while (dataTableReader.Read());

                _nextSet += _setSize;

                // generate a token
                setToken = Guid.NewGuid().ToString();

                // raise the event safely
                EventHandler<TokenEventArgs> temp = NextSetReadyEvent;
                if (temp != null)
                {
                    temp(this, new TokenEventArgs(setToken));
                }
            }
        }

        public void initialise()
        {
            _conn.Open();
            cmd = new MySqlCommand("SELECT * FROM el ORDER BY el_id ASC LIMIT ?start, ?limit");

            cmd.Connection = _conn;

            cmd.Prepare();



            retrieverThread.Start();
        }

        public Dictionary<int, string> retrieveNextSet(string token)
        {
            // check the token is the one for this set
            if(token != setToken)
                throw new SecurityException();
            
            Dictionary<int, string> ns;

            lock (padlock)
            {
                ns = this.nextSet;
                this.nextSet = null;
                this.setToken = "EMPTY";
            }

            retrieverThread.Start();

            return ns;
        }

        public event EventHandler<TokenEventArgs> NextSetReadyEvent;
    }
}
