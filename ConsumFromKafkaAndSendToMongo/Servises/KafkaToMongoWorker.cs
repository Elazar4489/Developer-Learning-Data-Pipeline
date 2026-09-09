using Confluent.Kafka;
using ConsumerAndSendToMongo.Models;
using ConsumFromKafkaAndSendToMongo.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Runtime;
using System.Text.Json;

public class KafkaToMongoWorker : BackgroundService
{
    private static readonly JsonSerializerOptions jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };
    private readonly IConsumer<string, string> _consumer;
    private readonly IMongoCollection<Respondent> _collection;
    private readonly KafkaSettings _kafkaSettings;
    private readonly ILogger<KafkaToMongoWorker> _logger;

    public KafkaToMongoWorker(
        IConsumer<string, string> consumer,
        IMongoCollection<Respondent> collection,
        IOptions<KafkaSettings> kafkaOptions,
        ILogger<KafkaToMongoWorker> logger)
    {
        _consumer = consumer;
        _collection = collection;
        _kafkaSettings = kafkaOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        _consumer.Subscribe(_kafkaSettings.Topic);

        const int batchSize = 1000; 
        var batch = new List<Respondent>(batchSize);
        ConsumeResult<string, string>? lastConsumeResult = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));

                if (consumeResult?.Message?.Value != null)
                {
                    var data = JsonSerializer.Deserialize<Respondent>(consumeResult.Message.Value, jsonOptions);
                    if (data != null)
                    {
                        batch.Add(data);
                        lastConsumeResult = consumeResult;
                    }
                }

                if (batch.Count >= batchSize || (batch.Count > 0 && consumeResult == null))
                {
                    await _collection.InsertManyAsync(batch, cancellationToken: stoppingToken);

                    if (lastConsumeResult != null)
                    {
                        _consumer.Commit(lastConsumeResult);
                    }

                    _logger.LogInformation("Flushed batch of {Count} documents to MongoDB.", batch.Count);
                    batch.Clear();
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka error: {Reason}", ex.Error.Reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing batch");
            }
        }

        if (batch.Count > 0)
        {
            await _collection.InsertManyAsync(batch, cancellationToken: CancellationToken.None);
            if (lastConsumeResult != null)
            {
                _consumer.Commit(lastConsumeResult);
            }
        }
    }
}