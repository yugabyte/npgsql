using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBTestUtils
{
    static readonly string YbdbPath = Environment.GetEnvironmentVariable("YBDB_PATH")
        ?? throw new ArgumentException("YBDB_PATH not initialized");

    public void ExecuteShellCommand(string argument, string message)
    {
        var arguments = YbdbPath + argument;
        Process? process = null;
        try
        {
            var startInfo = new ProcessStartInfo()
            {
                FileName = "/bin/bash",
                Arguments = " -c \"" + arguments + " \"",
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardInput = true,
                RedirectStandardError = true,
            };
            process = new Process() { StartInfo = startInfo };
            Console.WriteLine("Executing command to " + message);
            process.Start();

            var error = process.StandardError.ReadToEnd();
            process.WaitForExit();
            var output = process.StandardOutput.ReadToEnd();
            process.WaitForExit();

            Console.WriteLine("Output:" + output);
            if (!string.IsNullOrWhiteSpace(error))
                Console.WriteLine("Error:" + error);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception caught in process: {0}", ex);
            throw;
        }
        finally
        {
            process?.Close();
            process?.Dispose();
        }
    }

    protected static async Task VerifyOn(string server, int ExpectedCount)
    {
        var url = string.Format("http://{0}:{1}/rpcz", server, 13000);
        var client = new HttpClient();
        try
        {
            var response = await client.GetAsync(url);
            response.EnsureSuccessStatusCode();
            var responseBody = await response.Content.ReadAsStringAsync();
            var count = responseBody.Split("client backend");
            Console.WriteLine(server + ":" + (count.Length - 1));
            Assert.AreEqual(ExpectedCount, count.Length - 1);

            // Verify Local

           VerifyLocal(server, ExpectedCount);

        }
        catch (HttpRequestException e)
        {
            Console.WriteLine(e.Message);
        }
    }

    protected static void VerifyLocal(string server, int ExpectedCount)
    {
        Console.WriteLine("Client side verification:");

        var recorded = ClusterAwareDataSource.GetLoad(server);
        Console.WriteLine(server + ":" + recorded);
        Assert.AreEqual(ExpectedCount, recorded);

    }
}
