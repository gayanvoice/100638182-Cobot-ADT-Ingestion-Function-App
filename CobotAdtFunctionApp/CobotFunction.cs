using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventGrid;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Linq.Expressions;

namespace IotHubtoTwins
{
    public class IoTHubtoTwins
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        [FunctionName("IoTHubtoTwins")]
        // While async void should generally be used with caution, it's not uncommon for Azure function apps, since the function app isn't awaiting the task.
#pragma warning disable AZF0001 // Suppress async void error
        public async void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
#pragma warning restore AZF0001 // Suppress async void error
        {
            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
            else
            try
            {
                var cred = new DefaultAzureCredential();
                var client = new DigitalTwinsClient(new Uri(adtInstanceUrl), cred);
                log.LogInformation($"ADT service client connection created.");

                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                        log.LogInformation(eventGridEvent.Data.ToString());

                        JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                        Azure.JsonPatchDocument jsonPatchDocument = new Azure.JsonPatchDocument();
                        string deviceId = (string)deviceMessage["systemProperties"]["iothub-connection-device-id"];
                        switch (deviceId)
                        {
                            case "Cobot":
                                double cobotElapsedTime = (double)deviceMessage["body"]["ElapsedTime"];
                                jsonPatchDocument.AppendReplace("/ElapsedTime", cobotElapsedTime);
                                break;
                            case "Base":
                                double basePosition = (double)deviceMessage["body"]["position"];
                                double baseTemperature = (double)deviceMessage["body"]["temperature"];
                                double baseVoltage = (double)deviceMessage["body"]["voltage"];
                                jsonPatchDocument.AppendReplace("/position", basePosition);
                                jsonPatchDocument.AppendReplace("/temperature", baseTemperature);
                                jsonPatchDocument.AppendReplace("/voltage", baseVoltage);
                                break;
                            case "ControlBox":
                                double controlBoxVoltage = (double)deviceMessage["body"]["Voltage"];
                                jsonPatchDocument.AppendReplace("/Voltage", controlBoxVoltage);
                                break;
                            case "Elbow":
                                double elbowPosition = (double)deviceMessage["body"]["position"];
                                double elbowTemperature = (double)deviceMessage["body"]["temperature"];
                                double elbowVoltage = (double)deviceMessage["body"]["voltage"];
                                double elbowX = (double)deviceMessage["body"]["x"];
                                double elbowY = (double)deviceMessage["body"]["y"];
                                double elbowZ = (double)deviceMessage["body"]["z"];
                                jsonPatchDocument.AppendReplace("/position", elbowPosition);
                                jsonPatchDocument.AppendReplace("/temperature", elbowTemperature);
                                jsonPatchDocument.AppendReplace("/voltage", elbowVoltage);
                                jsonPatchDocument.AppendReplace("/x", elbowX);
                                jsonPatchDocument.AppendReplace("/y", elbowY);
                                jsonPatchDocument.AppendReplace("/z", elbowZ);
                                break;
                            case "Payload":
                                double payloadMass = (double)deviceMessage["body"]["mass"];
                                double payloadCogx = (double)deviceMessage["body"]["cogx"];
                                double payloadCogy = (double)deviceMessage["body"]["cogy"];
                                double payloadCogz = (double)deviceMessage["body"]["cogz"];
                                jsonPatchDocument.AppendReplace("/mass", payloadMass);
                                jsonPatchDocument.AppendReplace("/cogx", payloadCogx);
                                jsonPatchDocument.AppendReplace("/cogy", payloadCogy);
                                jsonPatchDocument.AppendReplace("/cogz", payloadCogz);
                                break;
                            default:
                                // code block
                                break;
                        }

                        log.LogInformation($"JsonPatchDocument: {jsonPatchDocument}");
                        await client.UpdateDigitalTwinAsync(deviceId, jsonPatchDocument);
                    }
            }
            catch (Exception ex)
            {
                log.LogError($"Error in ingest function: {ex.Message}");
            }
        }
    }
}