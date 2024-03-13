using Microsoft.Extensions.Configuration;

class Program
{
    static async Task Main()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .Build();

        YourClass userTaskInstance = new YourClass(configuration);
        userTaskInstance.StartTimer();

        // Keep the console window open
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }

}