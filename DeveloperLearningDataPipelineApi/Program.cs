using DeveloperLearningDataPipelineApi.Models;
using MongoDB.Driver;

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddControllers();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IMongoCollection<Respondent>>(sp =>
{
    var config = builder.Configuration.GetSection("Mongo");
    var client = new MongoClient(config["ConnectionString"]);
    var database = client.GetDatabase(config["DatabaseName"]);
    return database.GetCollection<Respondent>(config["CollectionName"]);
});


var app = builder.Build();


if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
