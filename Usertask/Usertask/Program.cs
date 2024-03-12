class Program
{
    static async Task Main()
    {
        YourClass userTaskInstance = new YourClass();
        userTaskInstance.StartTimer();

        // Keep the console window open
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
}