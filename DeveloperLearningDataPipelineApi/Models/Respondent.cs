using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
namespace DeveloperLearningDataPipelineApi.Models
{
    [BsonIgnoreExtraElements]
    public class Respondent
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string? Id { get; set; }

        public int ResponseId { get; set; }

        public string? Age { get; set; }

        public int? YearsCode { get; set; }

        public string? DevType { get; set; }

        public string? LearnCodeChoose { get; set; }

        public List<string>? LearnCode { get; set; }

        public string? LearnCodeAI { get; set; }

        public List<string>? AILearnHow { get; set; }

        public string? AISelect { get; set; }

        public string? AIAcc { get; set; }

        public string? AISent { get; set; }
    }
}
