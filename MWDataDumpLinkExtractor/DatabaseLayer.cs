using System;
using System.Collections.Generic;
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
