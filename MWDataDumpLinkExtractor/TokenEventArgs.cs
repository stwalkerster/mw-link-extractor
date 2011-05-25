using System;
using System.Collections.Generic;
using System.Text;

namespace MWDataDumpLinkExtractor
{
    class TokenEventArgs : EventArgs
    {
        public TokenEventArgs()
        {
            
        }

        public TokenEventArgs(string token)
        {
            this.token = token;    
        }

        public string token { get; set; }
    }
}
