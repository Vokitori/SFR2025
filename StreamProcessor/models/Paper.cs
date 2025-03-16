using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor.models
{
    internal class Paper
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string[] Authors { get; set; }
        public string[] Keywords { get; set; }
    }
}
