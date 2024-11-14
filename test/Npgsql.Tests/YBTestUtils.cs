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
    public void ExecuteShellCommand(string argument, ref string? _outputMessage, ref string? _errorMessage)
{
    var path = Environment.GetEnvironmentVariable("YBDB_PATH");
    var arguments = path + argument;
    // Set process variable
    // Provides access to local and remote processes and enables you to start and stop local system processes.
    Process? _Process = null;
    try
    {
        ProcessStartInfo startInfo = new ProcessStartInfo()
        {
            FileName = "/bin/bash",
            Arguments = " -c \"" + arguments + " \"",
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardInput = true,
            RedirectStandardError = true,
        };
        _Process = new Process()
        {
            StartInfo = startInfo,
        };
        _Process.Start();

        // Instructs the Process component to wait indefinitely for the associated process to exit.
        _errorMessage = _Process.StandardError.ReadToEnd();
        _Process.WaitForExit();

        // Instructs the Process component to wait indefinitely for the associated process to exit.
        _outputMessage = _Process.StandardOutput.ReadToEnd();
        _Process.WaitForExit();
    }
    catch (Exception _Exception)
    {
        // Error
        Console.WriteLine("Exception caught in process: {0}", _Exception.ToString());
    }
    finally
    {
        // close process and do cleanup
        _Process?.Close();
        _Process?.Dispose();
        _Process = null!;
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
