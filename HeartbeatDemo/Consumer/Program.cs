using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System.Text;
using System.Text.Json.Serialization;

namespace Consumer
{
    public class Program
    {
        public const string stateEventHubName = "state";

        static async Task Main(string[] args)
        {
            // Parse args for 2 values:
            //
            // 1. Blob checkpoint connection string
            // 2. Event Hub connection string
            //
            if (args.Length != 2)
            {
                System.Console.WriteLine("Missing 2 required arguments.");
                return;
            }

            string blobConnectionString = args[0];
            string eventHubConnectionString = args[1];

            // Create a cancellation token source to handle termination signals
            //
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true; // Cancel the default termination behavior
                cts.Cancel(); // Cancel the ongoing operation
            };

            // Create a blob container client that the event processor will use
            //
            BlobContainerClient storageClient = new BlobContainerClient(
                connectionString: blobConnectionString,
                blobContainerName: "checkpoint"
            );

            // Initiate Processor Client
            //
            var processorClient = new EventProcessorClient(
                checkpointStore: storageClient,
                consumerGroup: EventHubConsumerClient.DefaultConsumerGroupName,
                connectionString: eventHubConnectionString,
                eventHubName: stateEventHubName
            );

            // Initiate Consumer Client
            //
            var consumerClient = new EventHubConsumerClient(
                consumerGroup: EventHubConsumerClient.DefaultConsumerGroupName,
                connectionString: eventHubConnectionString,
                eventHubName: stateEventHubName
            );

            // Register handlers for processing events and handling errors
            //
            processorClient.ProcessEventAsync += async (eventArgs) =>
            {
                await ProcessEventHandler(eventArgs, consumerClient);
            };
            processorClient.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            //
            Console.WriteLine(
                $"Starting the processor, there are currently {GetNumEvents(consumerClient).Result} events in the queue."
            );
            await processorClient.StartProcessingAsync();

            // Wait for cancellation signal
            //
            Console.WriteLine("Processor started. Press Ctrl+C to stop.");

            try
            {
                await WaitForCancellationSignalAsync(cts.Token);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Hit Exception: {e}");
            }
            finally
            {
                await processorClient.StopProcessingAsync();
                await consumerClient.CloseAsync();
                Console.WriteLine("Exiting Processor.");
            }
        }

        static async Task<Task> ProcessEventHandler(
            ProcessEventArgs eventArgs,
            EventHubConsumerClient consumerClient
        )
        {
            string payload = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
            InstanceStatus? instanceStatusPayload =
                System.Text.Json.JsonSerializer.Deserialize<InstanceStatus>(payload);

            PrettyPrintInstanceStatus(
                instanceStatusPayload,
                eventArgs.Data.SequenceNumber,
                consumerClient
            );

            return Task.CompletedTask;
        }

        static void PrettyPrintInstanceStatus(
            InstanceStatus instanceStatusPayload,
            long offset,
            EventHubConsumerClient consumerClient
        )
        {
            if (instanceStatusPayload == null)
                return;

            if (instanceStatusPayload.HeartbeatStatus == "Healthy" && instanceStatusPayload.HealthStatus == "Healthy")
            {
                Console.ForegroundColor = ConsoleColor.Green;
            }
            else if (instanceStatusPayload.HeartbeatStatus == "Unhealthy")
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
            }
            else if (instanceStatusPayload.HealthStatus == "Unhealthy")
            {
                Console.ForegroundColor = ConsoleColor.Red;
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.DarkRed;
            }

            Console.WriteLine(
                $"[{offset}/{GetNumEvents(consumerClient).Result}] {instanceStatusPayload.LastStatusChangeTime} | Instance {instanceStatusPayload.InstanceId} | Heartbeat: {instanceStatusPayload.HeartbeatStatus}  | Engine Health: {instanceStatusPayload.HealthStatus}"
            );
            Console.ResetColor();
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            Console.WriteLine(
                $"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen."
            );
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }

        static async Task<long> GetNumEvents(EventHubConsumerClient consumerClient)
        {
            var partitionProperties = await consumerClient.GetPartitionPropertiesAsync("0");
            return partitionProperties.LastEnqueuedSequenceNumber;
        }

        static async Task WaitForCancellationSignalAsync(CancellationToken cancellationToken)
        {
            // Wait indefinitely or until cancellation signal
            //
            await Task.Delay(-1, cancellationToken);
        }

        public class InstanceStatus
        {
            [JsonPropertyName("instanceId")]
            public string InstanceId { get; set; }

            [JsonPropertyName("lastStatusChangeTime")]
            public DateTime LastStatusChangeTime { get; set; }

            [JsonPropertyName("heartbeatStatus")]
            public string HeartbeatStatus { get; set; }

            [JsonPropertyName("healthStatus")]
            public string HealthStatus { get; set; }

            [JsonPropertyName("healthMessage")]
            public string HealthMessage { get; set; }
        }
    }
}
