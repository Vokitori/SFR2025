using Avro;
using Avro.Specific;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

public class PaperMongoScheme 
{

    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public int Id { get; set; }
    [BsonElement("name")]
    public string Name { get; set; }
    [BsonElement("authors")]
    public List<string> Authors { get; set; }
    [BsonElement("keywords")]
    public List<string> Keywords { get; set; }
    [BsonElement("countryOfPublication")]
    public string CountryOfPublication { get; set; }

 
}