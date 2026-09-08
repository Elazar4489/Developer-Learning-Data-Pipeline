using DeveloperLearningDataPipelineApi.Models;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson;
using MongoDB.Driver;
namespace DeveloperLearningDataPipelineApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RespondentsController : ControllerBase
    {
        private readonly IMongoCollection<Respondent> _collection;

        public RespondentsController(IMongoCollection<Respondent> collection)
        {
            _collection = collection;
        }

        [HttpGet("technical-documentation")]
        public async Task<ActionResult<List<Respondent>>> GetByTechnicalDocumentation([FromQuery] int page = 1, [FromQuery] int limit = 50)
        {
            var filter = Builders<Respondent>.Filter.Regex(
                r => r.LearnCode,
                new BsonRegularExpression("Technical documentation", "i")
            );
            int skip = (page - 1) * limit;
            var results = await _collection.Find(filter).Skip(skip).Limit(limit).ToListAsync();
            return Ok(results);
        }

        [HttpGet("tech-docs-and-ai")]
        public async Task<ActionResult<List<Respondent>>> GetByTechDocsAndAi([FromQuery] int page = 1, [FromQuery] int limit = 50)
        {
            var builder = Builders<Respondent>.Filter;

            var techDocsFilter = builder.Regex(
                r => r.LearnCode,
                new BsonRegularExpression("Technical documentation", "i")
            );

            var aiLearningFilter = builder.Regex(
                r => r.LearnCodeAI,
                new BsonRegularExpression("^Yes", "i")
            );
            int skip = (page - 1) * limit;
            var combinedFilter = builder.And(techDocsFilter, aiLearningFilter);

            var results = await _collection
                .Find(combinedFilter)
                .Skip(skip)
                .Limit(limit)
                .ToListAsync();

            return Ok(results);
        }

        [HttpGet("by-ai-trust")]
        public async Task<ActionResult<List<Respondent>>> GetByAiTrust(
            [FromQuery] string category,
            [FromQuery] int page = 1,
            [FromQuery] int limit = 50)
        {
            if (string.IsNullOrWhiteSpace(category))
            {
                return BadRequest("Category parameter is required (e.g. 'Somewhat distrust', 'Highly trust').");
            }

            var filter = Builders<Respondent>.Filter.Regex(
                r => r.AIAcc,
                new BsonRegularExpression(category.Trim(), "i")
            );
            int skip = (page - 1) * limit;
            var results = await _collection
                .Find(filter)
                .Skip(skip)
                .Limit(limit)
                .ToListAsync();

            return Ok(results);
        }

        [HttpGet("by-experience")]
        public async Task<ActionResult<List<Respondent>>> GetByExperience(
            [FromQuery] int? minYears,
            [FromQuery] int? maxYears,
            [FromQuery] int page = 1,
            [FromQuery] int limit = 50)
        {
            var builder = Builders<Respondent>.Filter;
            var filters = new List<FilterDefinition<Respondent>>();

            if (minYears.HasValue)
            {
                filters.Add(builder.Gte(r => r.YearsCode, minYears.Value));
            }

            if (maxYears.HasValue)
            {
                filters.Add(builder.Lte(r => r.YearsCode, maxYears.Value));
            }

            if (filters.Count == 0)
            {
                return BadRequest("Please provide at least 'minYears' or 'maxYears'.");
            }

            var combinedFilter = builder.And(filters);
            int skip = (page - 1) * limit;

            var results = await _collection
                .Find(combinedFilter)
                .Skip(skip)
                .Limit(limit)
                .ToListAsync();

            return Ok(results);
        }

        [HttpGet("top-backend-ai-learners")]
        public async Task<ActionResult<List<Respondent>>> GetTopBackendAiLearners()
        {
            var filterBuilder = Builders<Respondent>.Filter;

            var backendFilter = filterBuilder.Regex(
                r => r.DevType,
                new BsonRegularExpression("back-end", "i")
            );

            var aiLearningFilter = filterBuilder.Regex(
                r => r.LearnCodeAI,
                new BsonRegularExpression("^Yes", "i")
            );

            var hasExperienceFilter = filterBuilder.Ne(r => r.YearsCode, null);

            var combinedFilter = filterBuilder.And(backendFilter, aiLearningFilter, hasExperienceFilter);

            var sort = Builders<Respondent>.Sort.Descending(r => r.YearsCode);

            var results = await _collection.Find(combinedFilter)
                                          .Sort(sort)
                                          .Limit(20)
                                          .ToListAsync();

            return Ok(results);
        }
    }
    
}
