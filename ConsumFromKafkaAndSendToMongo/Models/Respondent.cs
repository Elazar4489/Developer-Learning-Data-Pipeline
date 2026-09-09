using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ConsumFromKafkaAndSendToMongo.Models
{
    public class Respondent
    {
        public int ResponseId { get; set; }

        public string Age { get; set; } = string.Empty;
        public double? YearsCode { get; set; }

        // שדה נוח לשימוש בקוד שמחזיר 0 אם היה null
        [JsonIgnore]
        public double SafeYearsCode => YearsCode ?? 0;
        public string DevType { get; set; } = string.Empty;
        public string LearnCodeChoose { get; set; } = string.Empty;
        public List<string> LearnCode { get; set; } = new();
        public string LearnCodeAI { get; set; } = string.Empty;
        public List<string> AILearnHow { get; set; } = new();
        public string AISelect { get; set; } = string.Empty;
        public string AIAcc { get; set; } = string.Empty;
        public string AISent { get; set; } = string.Empty;
    }
}