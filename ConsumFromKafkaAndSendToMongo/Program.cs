using Confluent.Kafka;
using ConsumerAndSendToMongo.Models;
using ConsumFromKafkaAndSendToMongo.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<MongoSettings>(builder.Configuration.GetSection("Mongo"));

builder.Services.AddSingleton<IMongoClient>(sp =>
{
    var mongoSettings = sp.GetRequiredService<IOptions<MongoSettings>>().Value;
    return new MongoClient(mongoSettings.ConnectionString);
});

builder.Services.AddSingleton(sp =>
{
    var client = sp.GetRequiredService<IMongoClient>();
    var mongoSettings = sp.GetRequiredService<IOptions<MongoSettings>>().Value;
    var database = client.GetDatabase(mongoSettings.DatabaseName);
    return database.GetCollection<Respondent>(mongoSettings.CollectionName);
});

builder.Services.AddSingleton<IConsumer<string, string>>(sp =>
{
    var kafkaSettings = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;

    var config = new ConsumerConfig
    {
        BootstrapServers = kafkaSettings.BootstrapServers,
        GroupId = kafkaSettings.GroupId,
        AutoOffsetReset = Enum.Parse<AutoOffsetReset>(kafkaSettings.AutoOffsetReset, ignoreCase: true),
        EnableAutoCommit = kafkaSettings.EnableAutoCommit
    };

    return new ConsumerBuilder<string, string>(config).Build();
});

builder.Services.AddHostedService<KafkaToMongoWorker>();

var app = builder.Build();
await app.RunAsync();