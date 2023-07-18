using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventGrid;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace CobotADTIoTIngestionFunctionApp
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
                                double basePosition = (double)deviceMessage["body"]["Position"];
                                double baseTemperature = (double)deviceMessage["body"]["Temperature"];
                                double baseVoltage = (double)deviceMessage["body"]["Voltage"];
                                jsonPatchDocument.AppendReplace("/Position", basePosition);
                                jsonPatchDocument.AppendReplace("/Temperature", baseTemperature);
                                jsonPatchDocument.AppendReplace("/Voltage", baseVoltage);
                                break;
                            case "ControlBox":
                                double controlBoxVoltage = (double)deviceMessage["body"]["Voltage"];
                                jsonPatchDocument.AppendReplace("/Voltage", controlBoxVoltage);
                                break;
                            case "Elbow":
                                double elbowPosition = (double)deviceMessage["body"]["Position"];
                                double elbowTemperature = (double)deviceMessage["body"]["Temperature"];
                                double elbowVoltage = (double)deviceMessage["body"]["Voltage"];
                                double elbowX = (double)deviceMessage["body"]["X"];
                                double elbowY = (double)deviceMessage["body"]["Y"];
                                double elbowZ = (double)deviceMessage["body"]["Z"];
                                jsonPatchDocument.AppendReplace("/Position", elbowPosition);
                                jsonPatchDocument.AppendReplace("/Temperature", elbowTemperature);
                                jsonPatchDocument.AppendReplace("/Voltage", elbowVoltage);
                                jsonPatchDocument.AppendReplace("/X", elbowX);
                                jsonPatchDocument.AppendReplace("/Y", elbowY);
                                jsonPatchDocument.AppendReplace("/Z", elbowZ);
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
                            case "Shoulder":
                                double shoulderPosition = (double)deviceMessage["body"]["Position"];
                                double shoulderTemperature = (double)deviceMessage["body"]["Temperature"];
                                double shoulderVoltage = (double)deviceMessage["body"]["Voltage"];
                                jsonPatchDocument.AppendReplace("/Position", shoulderPosition);
                                jsonPatchDocument.AppendReplace("/Temperature", shoulderTemperature);
                                jsonPatchDocument.AppendReplace("/Voltage", shoulderVoltage);
                                break;
                            case "Wrist1":
                                double wrist1Position = (double)deviceMessage["body"]["Position"];
                                double wrist1Temperature = (double)deviceMessage["body"]["Temperature"];
                                double wrist1Voltage = (double)deviceMessage["body"]["Voltage"];
                                jsonPatchDocument.AppendReplace("/Position", wrist1Position);
                                jsonPatchDocument.AppendReplace("/Temperature", wrist1Temperature);
                                jsonPatchDocument.AppendReplace("/Voltage", wrist1Voltage);
                                break;
                            case "Wrist2":
                                double wrist2Position = (double)deviceMessage["body"]["Position"];
                                double wrist2Temperature = (double)deviceMessage["body"]["Temperature"];
                                double wrist2Voltage = (double)deviceMessage["body"]["Voltage"];
                                jsonPatchDocument.AppendReplace("/Position", wrist2Position);
                                jsonPatchDocument.AppendReplace("/Temperature", wrist2Temperature);
                                jsonPatchDocument.AppendReplace("/Voltage", wrist2Voltage);
                                break;
                            case "Wrist3":
                                double wrist3Position = (double)deviceMessage["body"]["Position"];
                                double wrist3Temperature = (double)deviceMessage["body"]["Temperature"];
                                double wrist3Voltage = (double)deviceMessage["body"]["Voltage"];
                                jsonPatchDocument.AppendReplace("/Position", wrist3Position);
                                jsonPatchDocument.AppendReplace("/Temperature", wrist3Temperature);
                                jsonPatchDocument.AppendReplace("/Voltage", wrist3Voltage);
                                break;
                            case "Tool":
                                double toolTemperature = (double)deviceMessage["body"]["Temperature"];
                                double toolVoltage = (double)deviceMessage["body"]["Voltage"];
                                double toolX = (double)deviceMessage["body"]["X"];
                                double toolY = (double)deviceMessage["body"]["Y"];
                                double toolZ = (double)deviceMessage["body"]["Z"];
                                double toolRx = (double)deviceMessage["body"]["Rx"];
                                double toolRy = (double)deviceMessage["body"]["Ry"];
                                double toolRz = (double)deviceMessage["body"]["Rz"];
                                jsonPatchDocument.AppendReplace("/Temperature", toolTemperature);
                                jsonPatchDocument.AppendReplace("/Voltage", toolVoltage);
                                jsonPatchDocument.AppendReplace("/X", toolX);
                                jsonPatchDocument.AppendReplace("/Y", toolY);
                                jsonPatchDocument.AppendReplace("/Z", toolZ);
                                jsonPatchDocument.AppendReplace("/Rx", toolRx);
                                jsonPatchDocument.AppendReplace("/Ry", toolRy);
                                jsonPatchDocument.AppendReplace("/Rz", toolRz);
                                break;
                            default:
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