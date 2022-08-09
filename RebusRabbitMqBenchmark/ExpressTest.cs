using BenchmarkDotNet.Attributes;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.RabbitMq;
using Headers = Rebus.Messages.Headers;

namespace RebusRabbitMqBenchmark;

[MinColumn, MaxColumn, HtmlExporter]
public class ExpressTest
{
    const string LocalConnectionString = "amqp://localhost";
    const string RemoteConnectionString = "amqps://(...remote connection string here...)";
    const string InputQueueName = "some-queue";

    IBus _localBus;
    IBus _remoteBus;

    [Params(1, 10, 100, 1000)]
    public int MessagesToPublish { get; set; }

    [Params(true, false)]
    public bool UseRemoteRabbitMq { get; set; }

    [Params(true, false)]
    public bool UseExpress { get; set; }

    [GlobalSetup]
    public void CreateBus()
    {
        _localBus = Configure.With(new BuiltinHandlerActivator())
            .Logging(l => l.None())
            .Transport(t => t.UseRabbitMq(LocalConnectionString, InputQueueName))
            .Options(o => o.SetNumberOfWorkers(0))
            .Start();

        _remoteBus = Configure.With(new BuiltinHandlerActivator())
            .Logging(l => l.None())
            .Transport(t => t.UseRabbitMq(RemoteConnectionString, InputQueueName))
            .Options(o => o.SetNumberOfWorkers(0))
            .Start();
    }

    [GlobalCleanup]
    public void ShutdownBus()
    {
        _localBus.Dispose();
        _remoteBus.Dispose();
    }

    [IterationCleanup]
    public void PurgeInputQueue()
    {
        void PurgeQueue(string connectionString)
        {
            using var localTransport = new RabbitMqTransport(connectionString, InputQueueName, new NullLoggerFactory());

            localTransport.PurgeInputQueue();
        }

        PurgeQueue(LocalConnectionString);
        PurgeQueue(RemoteConnectionString);
    }

    [Benchmark]
    public async Task SendMessages()
    {
        var headers = UseExpress
            ? new Dictionary<string, string> { [Headers.Express] = "" }
            : new Dictionary<string, string>();

        var bus = UseRemoteRabbitMq ? _remoteBus : _localBus;

        foreach (var msg in Enumerable.Range(0, MessagesToPublish))
        {
            await bus.SendLocal($"THIS IS MESSAGE {msg}", headers);
        }
    }
}