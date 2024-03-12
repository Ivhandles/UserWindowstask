using System.Text;
using System;
using Azure.Storage.Blobs;
using Newtonsoft.Json;
using Azure.Messaging.EventHubs.Producer;

public class YourClass
{
    private const string connectionString = "Endpoint=sb://amdocs-b2b.servicebus.windows.net/;SharedAccessKeyName=amdox-eventhub;SharedAccessKey=VWZ6DMYNQvbRSSuRIghlyg36XzXWdNC72+AEhFaXWGw=;EntityPath=amdox-eventhub";
    private const string eventHubName = "amdox-eventhub";
    private const string blobconnectionString = "DefaultEndpointsProtocol=https;AccountName=amdox;AccountKey=EsOwsWTExYkxhSDuyhUJ1Ls0yCLjKI/ULQo92BGPXs2xgyy0nQsOCqwRdY3g9FKAogOFGYV6xrzH+AStDwsqaw==;EndpointSuffix=core.windows.net";
    private const string blobcontainerName = "amdox-container";
    private const string blobName = "initialdb.json";

    public void StartTimer()
    {
        // Set up a timer to execute RunTask every minute
        Timer timer = new Timer(RunTask, null, TimeSpan.Zero, TimeSpan.FromMinutes(30));
    }
    public void RunTask(object state)
    {
        string filePath = @"C:\Users\vdeek\Downloads\userinput6.json";
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

                await UpdateorInsertBlobData(userList, blobData);

                var result = UploadtoBlob(blobData);

                var pollingblobData = await ReadJsonFromBlobAsync();

                List<Initialjsonstruct> unsyncedList = pollingblobData.Where(item => !item.IsSynced).ToList();

                var eventStatus = PostEvent(unsyncedList);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error retrieving data from blob storage: {ex.Message}");
            // Log the exception as needed
            // Optionally rethrow the exception if you want to propagate it
        }
    }

    public async Task<List<Initialjsonstruct>> ReadJsonFromBlobAsync()
    {
        try
        {


            BlobServiceClient blobServiceClient = new BlobServiceClient(blobconnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobcontainerName);
            BlobClient blobClient = containerClient.GetBlobClient(blobName);

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
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            foreach (var user in userList)
            {
                string jsonUser = JsonConvert.SerializeObject(user);


                byte[] eventDataBytes = Encoding.UTF8.GetBytes(jsonUser);


                Azure.Messaging.EventHubs.EventData eventData = new Azure.Messaging.EventHubs.EventData(eventDataBytes);
                eventsToSend.Add(eventData);
            }


            await producerClient.SendAsync(eventsToSend.ToArray());
            await producerClient.DisposeAsync();
            Console.WriteLine($"Successfully sent {eventsToSend.Count} events to Event Hub.");
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


        BlobServiceClient blobServiceClient = new BlobServiceClient(blobconnectionString);


        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobcontainerName);



        BlobClient blobClient = containerClient.GetBlobClient(blobName);

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