using RedisSemaphore;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace RedisSemaphoreTests
{
    public class RedisSemaphoreTests
    {
        private static readonly ConnectionMultiplexer connection = ConnectionMultiplexer.Connect("localhost:6379");

        [Fact]
        public async void MultipleUpdates_UpdatesOnce()
        {
            var semaphoreProvider = new SemaphoreProvider(connection);

            var testId = Guid.NewGuid().ToString();
            var semaphoreName = "semaphoreTests:SemaphoreName:" + testId;
            var resourceName = "semaphoreTests:ResourceName:" + testId;

            const int semaphoreCount = 5;
            const int iterations = 10_000;

            var db = connection.GetDatabase();

            var tasks = new List<Task>(iterations);

            for (int i = 0; i < iterations; i++)
            {
                tasks.Add(Increment());
            }

            await Task.WhenAll(tasks);

            var result = await db.StringGetAsync(resourceName);

            Assert.Equal(semaphoreCount, result);

            async Task Increment()
            {
                var semaphore = await semaphoreProvider.TryGetSemaphore(semaphoreName, semaphoreCount, TimeSpan.FromSeconds(10));

                if (!semaphore.wasAcquired) return;

                try
                {
                    await db.StringIncrementAsync(resourceName);
                    await Task.Delay(5000);
                }
                finally
                {
                    await semaphoreProvider.Release(semaphore.ctx);
                }
            }
        }
    }
}
