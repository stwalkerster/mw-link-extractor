using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Xml;
using System.Xml.XPath;

namespace TableSplitter
{
    /// <summary>
    /// Provides an interface for using the .hmbot configuration files
    /// </summary>
    public class DotHmbotConfigurationFile
    {
        /// <summary>
        /// XML Root namespace/schema/something...
        /// </summary>
        public const string NS_ROOT = "";

        /// <summary>
        /// MySQL constants for use with the configuration. Provided for backwards compatiability
        /// </summary>
        public const string MYSQL_SERVER = "mySqlServerHostname",
                            MYSQL_PORT = "mySqlServerPort",
                            MYSQL_USERNAME = "mySqlUsername",
                            MYSQL_PASSWORD = "mySqlPassword",
                            MYSQL_SCHEMA = "mySqlSchema";

        private string filename = "";
        readonly Dictionary<string, string> _store = new Dictionary<string, string>();

        private void readVer1ConfigFile()
        {
            StreamReader settingsreader = new StreamReader(filename);
            this[MYSQL_SERVER] = settingsreader.ReadLine();
            this[MYSQL_PORT] = settingsreader.ReadLine();
            this[MYSQL_USERNAME] = settingsreader.ReadLine();
            this[MYSQL_PASSWORD] = settingsreader.ReadLine();
            this[MYSQL_SCHEMA] = settingsreader.ReadLine();
            settingsreader.Close();

        }

        private DotHmbotConfigurationFile()
        {
        }



        ///<summary>
        /// Setting store
        ///</summary>
        ///<param name="key">Setting name</param>
        public string this[string key]
        {
            get
            {
                return _store[key];
            }
            set
            {
                _store[key] = value;
            }
        }

        public static DotHmbotConfigurationFile Create(string filename, string serverHostname, uint serverPort,
                                        string username, string password, string schema)
        {
            DotHmbotConfigurationFile x = new DotHmbotConfigurationFile { filename = filename };
            x[MYSQL_SERVER] = serverHostname;
            x[MYSQL_PORT] = serverPort.ToString();
            x[MYSQL_USERNAME] = username;
            x[MYSQL_PASSWORD] = password;
            x[MYSQL_SCHEMA] = schema;

            x.save();

            return x;
        }

        public static DotHmbotConfigurationFile Create(string filename)
        {
            DotHmbotConfigurationFile x = new DotHmbotConfigurationFile { filename = filename };

            x.save();

            return x;
        }

        public static DotHmbotConfigurationFile Open(string filename)
        {
            DotHmbotConfigurationFile x = new DotHmbotConfigurationFile { filename = filename };


            StreamReader sr = new StreamReader(filename);
            string lineone = sr.ReadLine();
            sr.Close();

            if (lineone.Contains("<?xml"))
            {
                x.readXmlConfigFile();
            }
            else
            {
                x.readVer1ConfigFile();
            }

            return x;
        }

        private void readXmlConfigFile()
        {
            XPathDocument xpd = new XPathDocument(filename);
            XPathNavigator xpn = xpd.CreateNavigator();
            XPathNodeIterator xpni = xpn.Select("/hmbot[@version='2']/config/option");
            while (xpni.MoveNext())
            {
                _store[xpni.Current.GetAttribute("name", "")] = xpni.Current.Value;
            }
        }

        public void save()
        {
            XmlDocument doc = new XmlDocument();
            doc.AppendChild(doc.CreateXmlDeclaration("1.0", null, null));
            XmlElement hmbot = doc.CreateElement("hmbot", NS_ROOT);
            XmlElement config = doc.CreateElement("config");
            hmbot.SetAttribute("version", "2");
            hmbot.AppendChild(config);
            doc.AppendChild(hmbot);

            foreach (KeyValuePair<string, string> item in _store)
            {
                XmlElement option = doc.CreateElement("option");
                option.SetAttribute("name", item.Key);
                option.AppendChild(doc.CreateTextNode(item.Value));
                config.AppendChild(option);
            }

            XmlTextWriter xtw = new XmlTextWriter(filename, Encoding.ASCII) { Formatting = Formatting.Indented };
            doc.WriteTo(xtw);
            xtw.Flush();
            xtw.Close();
        }
    }
}