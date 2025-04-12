using Avro;
using Avro.Specific;
using System.Collections.Generic;

public class Paper : ISpecificRecord
{
    public static Schema _SCHEMA = Schema.Parse(@"
    {
        ""type"": ""record"",
        ""name"": ""ResearchPaper"",
        ""fields"": [
            { ""name"": ""Id"", ""type"": ""int"" },
            { ""name"": ""Name"", ""type"": ""string"" },
            { ""name"": ""Authors"", ""type"": { ""type"": ""array"", ""items"": ""string"" } },
            { ""name"": ""Keywords"", ""type"": { ""type"": ""array"", ""items"": ""string"" } },
            { ""name"": ""CountryOfPublication"", ""type"": ""string"" }
        ]
    }");

    public Schema Schema => _SCHEMA;

    public int Id { get; set; }
    public string Name { get; set; }
    public List<string> Authors { get; set; }
    public List<string> Keywords { get; set; }
    public string CountryOfPublication { get; set; }

    public object Get(int fieldPos)
    {
        return fieldPos switch
        {
            0 => Id,
            1 => Name,
            2 => Authors,
            3 => Keywords,
            4 => CountryOfPublication,
            _ => throw new AvroRuntimeException("Unknown field position")
        };
    }

    public void Put(int fieldPos, object value)
    {
        switch (fieldPos)
        {
            case 0: Id = (int)value; break;
            case 1: Name = (string)value; break;
            case 2: Authors = (List<string>)value; break;
            case 3: Keywords = (List<string>)value; break;
            case 4: CountryOfPublication = (string)value; break;
            default: throw new AvroRuntimeException("Unknown field position");
        }
    }
}