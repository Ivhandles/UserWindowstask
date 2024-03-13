using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs;
using System.Text.Json;
using JsonSerializer = System.Text.Json.JsonSerializer;
using Microsoft.Extensions.Configuration;

public class YourClass
{
   
    
 
    


    private readonly string blobConnectionString;
    private readonly string blobContainerName;
    private readonly string initialblobName;
    private readonly string eventHubConnectionString;
    private readonly string eventHubName;
    private readonly string replyeventHubConnectionString;
    private readonly string replyeventHubName;
    public YourClass(IConfiguration configuration)
    {
        blobConnectionString = configuration["BlobStorageSettings:BlobConnectionString"];

        blobContainerName = configuration["BlobStorageSettings:BlobContainerName"];
        initialblobName = configuration["BlobStorageSettings:BlobName"];
        eventHubConnectionString = configuration["EventHubSettings:EventHubConnectionString"];
        eventHubName = configuration["EventHubSettings:EventHubName"];
        replyeventHubConnectionString = configuration["ReplyEventHubSettings:ReplyEventHubConnectionString"];
        replyeventHubName = configuration["ReplyEventHubSettings:ReplyEventHubName"];


    }

   

    public void StartTimer()
    {
        // Set up a timer to execute RunTask every minute
        Timer timer = new Timer(RunTask, null, TimeSpan.Zero, TimeSpan.FromMinutes(30));
    }
    public void RunTask(object state)
    {
        string filePath = @"C:\Users\ivoyant.DESKTOP-GBDO8ON\Downloads\newuserinput.json";
        Task.Run(() => PostJsonFileAsync(filePath)).Wait();
    }

    public async Task PostJsonFileAsync(string filePath)
    {
        if (string.IsNullOrEmpty(filePath) || !File.Exists(filePath))
        {
            throw new ArgumentException("File path is null, empty, or does not exist.");
        }

        try
        {
            using (var streamReader = new StreamReader(filePath))
            {
                var jsonContent = await streamReader.ReadToEndAsync();

                var userList = System.Text.Json.JsonSerializer.Deserialize<List<Initialjsonstruct>>(jsonContent);

                var blobData = await ReadJsonFromBlobAsync();

                if (blobData.Count == 0)
                {
                    // If blobData is null, upload the userList directly
                    var result = UploadtoBlob(userList);
                }
                else
                {
                    // If blobData is not null, update or insert the data
                    await UpdateorInsertBlobData(userList, blobData);
                    var result = UploadtoBlob(blobData);
                }



                var pollingblobData = await ReadJsonFromBlobAsync();

                List<Initialjsonstruct> unsyncedList = pollingblobData.Where(item => !item.IsSynced).ToList();

                await PostEvent(unsyncedList);
                var events = await PollEventHubAsync(async eventData =>
                {
                    // Your custom logic to handle the event (e.g., store it in a variable)
                    Console.WriteLine("Event received: " + eventData);
                });
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error retrieving data from blob storage: {ex.Message}");
            // Log the exception as needed
            // Optionally rethrow the exception if you want to propagate it
        }
    }





    public async Task<List<string>> PollEventHubAsync(Func<string, Task> eventHandler)
    {
        var _storageConnectionString = blobConnectionString;
        var BlobcontainerName = blobContainerName;
        var replyeventconnectionstring = replyeventHubConnectionString;
        var replyeventhubname = replyeventHubName;
        var receivedEvents = new List<string>();
        BlobContainerClient storageClient = new
       BlobContainerClient(
           _storageConnectionString,
           BlobcontainerName
       );

        var eventProcessorClient = new EventProcessorClient(
            storageClient,
            "$Default",
            replyeventconnectionstring,
            replyeventhubname);

        eventProcessorClient.ProcessEventAsync += ProcessEventHandler;
        eventProcessorClient.ProcessErrorAsync += ProcessErrorHandler;
        await eventProcessorClient.StartProcessingAsync();
        Console.WriteLine("Started the processor");
        await Task.Delay(TimeSpan.FromMinutes(8));
        await eventProcessorClient.StopProcessingAsync();
        Console.WriteLine("Stop the processor");


        foreach (var successEvent in successEvents)
        {
            Console.WriteLine(successEvent);
        }


        return receivedEvents;

    }




    public List<string> successEvents = new List<string>();
    public List<string> userGuidList = new List<string>();
    public List<Initialjsonstruct> retriveddata = new List<Initialjsonstruct>();

    public async Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
        try
        {
            var eventData = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());

            // Check if the event data contains "Value":"Success"
            if (eventData.Contains("\"Value\":\"Success\""))
            {


                // Add the event data to the successEvents list
                await AddToSuccessEventsAsync(eventData);
              
                // Asynchronously update the checkpoint
                await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
            }
            else
            {
                // Skip processing if the event does not meet the criteria
                Console.WriteLine("\tSkipped event: {0}", eventData);
            }
            Console.WriteLine("Total success events count: {0}", successEvents.Count);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An exception occurred while processing events: {ex.Message}");
            // Handle the exception as needed
        }
    }



    public async Task AddToSuccessEventsAsync(string eventData)
    {
        successEvents.Add(eventData);

        // Parse UserGuid from eventData and add it to the userGuidList
        string userGuid = GetUserGuidFromEventData(eventData);
        if (!string.IsNullOrEmpty(userGuid))
        {
            userGuidList.Add(userGuid);

        }

      


        retriveddata = await ReadJsonFromBlobAsync();
     
        foreach (var entry in retriveddata)
        {
            if (userGuidList.Contains(entry.UserGuid))
            {

                entry.IsSynced = true;

            }
        }


        await UpdateJsonInBlobAsync(retriveddata);
      
    }

    public async Task UpdateJsonInBlobAsync(List<Initialjsonstruct> updatedData)
    {
        try
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(blobConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            BlobClient blobClient = containerClient.GetBlobClient(initialblobName);

            // Serialize the updated data to JSON
            string updatedJsonData = JsonConvert.SerializeObject(updatedData);

            // Convert the JSON data to bytes
            byte[] updatedBytes = Encoding.UTF8.GetBytes(updatedJsonData);

            // Create a memory stream from the byte array
            using (MemoryStream stream = new MemoryStream(updatedBytes))
            {
                // Upload the updated data to the blob, overwriting the existing content
                await blobClient.UploadAsync(stream, true);
                Console.WriteLine($"User data updated successfully in blob storage.");

            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error updating data in blob storage: {ex.Message}");
            throw;
        }
    }

    public string GetUserGuidFromEventData(string eventData)
    {
        var keyIndex = eventData.IndexOf("\"Key\":\"") + 7;
        var userGuidStartIndex = keyIndex;
        var userGuidEndIndex = eventData.IndexOf("\"", userGuidStartIndex);

        return eventData.Substring(userGuidStartIndex, userGuidEndIndex - userGuidStartIndex);
    }

    public Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
    {
        Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");

        Console.WriteLine(eventArgs.Exception.Message);

        return Task.CompletedTask;
    }



    public async Task<List<Initialjsonstruct>> ReadJsonFromBlobAsync()
    {
        try
        {


            BlobServiceClient blobServiceClient = new BlobServiceClient(blobConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            BlobClient blobClient = containerClient.GetBlobClient(initialblobName);

            if (await blobClient.ExistsAsync())
            {
                using (MemoryStream stream = new MemoryStream())
                {
                    await blobClient.DownloadToAsync(stream);
                    stream.Seek(0, SeekOrigin.Begin);

                    using (var reader = new StreamReader(stream))
                    {
                        var content = await reader.ReadToEndAsync();
                        var fetcheduserList = JsonConvert.DeserializeObject<List<Initialjsonstruct>>(content);



                        return fetcheduserList;
                    }
                }
            }
            else
            {
                return new List<Initialjsonstruct>();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error retrieving data from blob storage: {ex.Message}");
            throw;
        }
    }
    public async Task PostEvent(List<Initialjsonstruct> userList)
    {
        EventHubProducerClient producerClient = null;
        List<Azure.Messaging.EventHubs.EventData> eventsToSend = new List<Azure.Messaging.EventHubs.EventData>();

        try
        {
            producerClient = new EventHubProducerClient(eventHubConnectionString, eventHubName);

            foreach (var user in userList)
            {
                string jsonUser = JsonConvert.SerializeObject(user);


                byte[] eventDataBytes = Encoding.UTF8.GetBytes(jsonUser);


                Azure.Messaging.EventHubs.EventData eventData = new Azure.Messaging.EventHubs.EventData(eventDataBytes);
                eventsToSend.Add(eventData);
            }


            await producerClient.SendAsync(eventsToSend.ToArray());
            await producerClient.DisposeAsync();
            Console.WriteLine($"Successfully sent {eventsToSend.Count} events to Event Hub. User update Requets");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error sending events: {ex.Message}");
        }

    }
    public async Task UpdateorInsertBlobData(List<Initialjsonstruct> userList, List<Initialjsonstruct> blobData)
    {
        foreach (var user in userList)
        {
            var existingUserIndex = blobData.FindIndex(u => u.UserGuid == user.UserGuid);

            if (existingUserIndex != -1)
            {
                if (!AreUsersEqual(user, blobData[existingUserIndex]))
                {

                    blobData[existingUserIndex] = user;
                }
            }
            else
            {

                blobData.Add(user);
            }
        }
    }
    private bool AreUsersEqual(Initialjsonstruct user1, Initialjsonstruct user2)
    {
        return user1.UserGuid == user2.UserGuid &&
               user1.B2BUserld == user2.B2BUserld &&
               user1.B2BAccessCode == user2.B2BAccessCode &&
               user1.UttUID == user2.UttUID &&
               user1.Type == user2.Type &&
               user1.UserName == user2.UserName &&
               user1.FirstName == user2.FirstName &&
               user1.LastName == user2.LastName &&
               user1.FullName == user2.FullName &&
               user1.Email == user2.Email &&
               user1.IsSynced == user2.IsSynced &&
               user1.SyncStatus == user2.SyncStatus &&
               user1.ModificationDate == user2.ModificationDate &&
               user1.ModificationBatch == user2.ModificationBatch &&
               user1.SyncDate == user2.SyncDate;
    }
    private async Task UploadtoBlob(List<Initialjsonstruct> userList)
    {

        string jsonData = JsonConvert.SerializeObject(userList);



        BlobServiceClient blobServiceClient = new BlobServiceClient(blobConnectionString);


        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);




        BlobClient blobClient = containerClient.GetBlobClient(initialblobName);

        byte[] bytes = System.Text.Encoding.UTF8.GetBytes(jsonData);

        using (MemoryStream stream = new MemoryStream(bytes))
        {
            await blobClient.UploadAsync(stream, true);
            Console.WriteLine($"Data uploaded successfully to blob storage.");
        }
    }
}

public class Initialjsonstruct
{
    public string UserGuid { get; set; }
    public string B2BUserld { get; set; }
    public string B2BAccessCode { get; set; }
    public string UttUID { get; set; }
    public string Type { get; set; }
    public string UserName { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string FullName { get; set; }
    public string Email { get; set; }
    public bool IsSynced { get; set; }
    public string SyncStatus { get; set; }
    public string ModificationDate { get; set; }
    public string ModificationBatch { get; set; }
    public string SyncDate { get; set; }
}