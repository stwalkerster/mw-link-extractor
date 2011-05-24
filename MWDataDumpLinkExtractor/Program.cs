using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml.XPath;
namespace MWDataDumpLinkExtractor
{
    class Program
    {
        static void Main(string[] args)
        {
            Stream infile;
            StreamWriter outfile;


            switch (args.Length)
            {
                case 0:
                    infile = Console.OpenStandardInput();
                    outfile = new StreamWriter("urls.out");
                    break;
                case 1:

                    throw new ArgumentException();
                default:
                    infile = new FileStream(args[0], FileMode.Open);
                    outfile = new StreamWriter(args[1]);
                    break;
            }


            StreamReader infilereader = new StreamReader(infile);
            infilereader.
        }
    }
}