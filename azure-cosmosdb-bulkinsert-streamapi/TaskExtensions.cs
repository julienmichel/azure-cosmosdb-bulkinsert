using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace azure_cosmosdb_bulkinsert_streamapi
{
    public static class TaskExtensions
    {
        public static ConfiguredTaskAwaitable IgnoreCapturedContext(this Task awaitable)
        {
            if (awaitable == null) { throw new ArgumentNullException(nameof(awaitable)); }

            return awaitable.ConfigureAwait(continueOnCapturedContext: false);
        }

        public static ConfiguredTaskAwaitable<T> IgnoreCapturedContext<T>(this Task<T> awaitable)
        {
            if (awaitable == null) { throw new ArgumentNullException(nameof(awaitable)); }

            return awaitable.ConfigureAwait(continueOnCapturedContext: false);
        }
    }
}
