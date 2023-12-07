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
            // 1. Health Probe REST API endpoint
            // 2. Event Hub connection string
            //
            if (args.Length != 2)
            {
                System.Console.WriteLine("Missing 2 required arguments.");
                return;
            }

            string arcSqlServerHealthProbeEndpoint = args[0];
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

            // Initiate HTTP Client to pull from self-signed cert endpoint
            //
            var handler = new HttpClientHandler()
            {
                ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) =>
                {
                    return true;
                }
            };
            var httpClient = new HttpClient(handler);

            int eventCount = 1;
            while (!cts.Token.IsCancellationRequested)
            {
                var response = await httpClient.GetAsync(arcSqlServerHealthProbeEndpoint);
                var responseContent = await response.Content.ReadAsStringAsync();
                var healthzContent = JsonConvert.DeserializeObject<ApiResponse>(responseContent);
                byte[] messageBytes = Encoding.UTF8.GetBytes(
                    JsonConvert.SerializeObject(responseContent)
                );

                if (healthzContent.InstanceStatus.InstanceReachable)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                }

                Console.WriteLine($".");
                Console.ResetColor();

                await producerClient.SendAsync(new List<EventData> { new EventData(messageBytes) });
                eventCount++;

                await Task.Delay(heartbeatIntervalInMilliSeconds);
            }

            // Close the clients
            //
            await producerClient.CloseAsync();

            Console.BackgroundColor = ConsoleColor.Yellow;
            Console.ForegroundColor = ConsoleColor.Black;
            Console.WriteLine($"Probing Worker going offline.");
            Console.ResetColor();
        }
    }
}
