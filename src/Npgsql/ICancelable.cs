using System;
using System.Threading.Tasks;

namespace YBNpgsql;

interface ICancelable : IDisposable, IAsyncDisposable
{
    void Cancel();

    Task CancelAsync();
}