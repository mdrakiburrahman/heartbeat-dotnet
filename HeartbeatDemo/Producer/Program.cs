using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json;
using System.Text;

namespace Producer
{
    public class Program
    {
        public const string heartbeatEventHubName = "heartbeat";
        public const int heartbeatIntervalInMilliSeconds = 500;

        static async Task Main(string[] args)
        {
            // Parse args for 2 values:
            //
            // 1. Arc SQL Server name
            // 2. Event Hub connection string
            //
            if (args.Length != 2)
            {
                System.Console.WriteLine("Missing 2 required arguments.");
                return;
            }

            string arcSqlServerName = args[0];
            string eventHubConnectionString = args[1];

            // Create a cancellation token source to handle termination signals
            //
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true; // Cancel the default termination behavior
                cts.Cancel(); // Cancel the ongoing operation
            };

            // Initiate Producer Client
            //
            var producerClient = new EventHubProducerClient(
                connectionString: eventHubConnectionString,
                eventHubName: heartbeatEventHubName
            );

            int eventCount = 1;
            while (!cts.Token.IsCancellationRequested)
            {
                var messageBody = new { machine_name = arcSqlServerName, machine_time = DateTime.UtcNow };
                byte[] messageBytes = Encoding.UTF8.GetBytes(
                    JsonConvert.SerializeObject(messageBody)
                );

                Console.WriteLine($".");

                await producerClient.SendAsync(new List<EventData> { new EventData(messageBytes) });
                eventCount++;

                await Task.Delay(heartbeatIntervalInMilliSeconds);
            }

            // Close the clients
            //
            await producerClient.CloseAsync();

            Console.BackgroundColor = ConsoleColor.Yellow;
            Console.ForegroundColor = ConsoleColor.Black;
            Console.WriteLine($"{arcSqlServerName} going offline.");
            Console.ResetColor();
        }
    }
}
