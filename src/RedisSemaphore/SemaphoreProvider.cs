using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace RedisSemaphore
{
    /// <summary>
    /// Provides disitributed non-blocking semaphores via Redis
    /// 
    /// https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-3-counting-semaphores/
    /// </summary>
    public class SemaphoreProvider
    {
        private readonly IConnectionMultiplexer multiplexer;
        private readonly int database;

        /// <summary>
        /// Construct a Semaphore provider from a redis connection and the database to connect to
        /// </summary>
        /// <param name="multiplexer"></param>
        /// <param name="database"></param>
        public SemaphoreProvider(IConnectionMultiplexer multiplexer, int database = 0)
        {
            this.multiplexer = multiplexer;
            this.database = database;
        }

        /// <summary>
        /// Attempt to acquire a semaphore
        /// </summary>
        /// <param name="semaphoreName">The name of the semaphore</param>
        /// <param name="semaphoreCount">The maximum size of the semaphore</param>
        /// <param name="timeout">How long the semaphore can be held before automatically releasing</param>
        /// <param name="identifier">The unique id of the semaphore holder, defaults to a random guid when not provided</param>
        /// <returns></returns>
        public async Task<(bool wasAcquired, SemaphoreContext ctx)> TryGetSemaphore(
            string semaphoreName,
            long semaphoreCount,
            TimeSpan timeout,
            string identifier = null)
        {
            if (string.IsNullOrEmpty(semaphoreName)) throw new ArgumentException($"{nameof(semaphoreName)} cannot be empty");
            if (semaphoreCount <= 0) throw new ArgumentOutOfRangeException($"{nameof(semaphoreCount)} must be greater than zero");

            identifier = identifier ?? Guid.NewGuid().ToString();

            var db = multiplexer.GetDatabase(database);

            var lockName = semaphoreName + ":lock";
            RedisValue token = Guid.NewGuid().ToString();

            if (db.LockTake(lockName, token, timeout))
            {
                try
                {
                    return await TryGetSemaphoreUnsafe(db, semaphoreName, semaphoreCount, timeout, identifier);
                }
                finally
                {
                    db.LockRelease(lockName, token);
                }
            }

            // we couldn't get the lock to try and get a spot in the semaphore!
            return (false, null);
        }

        /// <summary>
        /// Attempts to get a spot in the semaphore without first holding a lock
        /// </summary>
        /// <param name="semaphoreName"></param>
        /// <param name="semaphoreCount"></param>
        /// <param name="timeout"></param>
        /// <param name="identifier"></param>
        /// <returns></returns>
        private async Task<(bool wasAcquired, SemaphoreContext ctx)> TryGetSemaphoreUnsafe(
            IDatabase db,
            string semaphoreName,
            long semaphoreCount,
            TimeSpan timeout,
            string identifier)
        {
            var counterSetName = semaphoreName + ":owner";
            var counterName = semaphoreName + ":counter";

            var now = DateTime.UtcNow;
            var currentScore = ScoreDateTime(now);

            // Anything before the cutoff score has now expired
            var cutOffScore = ScoreDateTime(now - timeout);

            // remove any entries that no longer have a valid semaphore
            _ = await db.SortedSetRemoveRangeByScoreAsync(semaphoreName, 0, cutOffScore);

            // propogate any removed entries to the owner set from the timer set
            _ = await db.SortedSetCombineAndStoreAsync(
                SetOperation.Intersect, counterSetName, new RedisKey[] { counterSetName, semaphoreName }, new double[] { 1, 0 });

            // increment the counter and get the latest value
            var counter = await db.StringIncrementAsync(counterName);

            // add the current time to the timer set
            await db.SortedSetAddAsync(semaphoreName, identifier, currentScore);

            // add the current count to the owner set
            await db.SortedSetAddAsync(counterSetName, identifier, counter);

            // check our position in the list
            var rank = await db.SortedSetRankAsync(counterSetName, identifier);

            var ctx = new SemaphoreContext
            {
                SemaphoreName = semaphoreName, Identifier = identifier, CounterSet = counterSetName
            };

            // our rank is low enough (lower is better) so the caller has a spot in the semaphore
            if (rank.HasValue && rank.Value < semaphoreCount) return (true, ctx);

            // we are too far up the semaphore list to be allowed access to the semaphore
            // release our handle to the semaphore and fail the call
            await Release(ctx);

            return (false, null);
        }

        /// <summary>
        /// Extend the semaphore
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<bool> TryExtendSemaphore(SemaphoreContext context)
        {
            var db = multiplexer.GetDatabase(database);

            var currentScore = ScoreDateTime(DateTime.UtcNow);

            var wasAdded = await db.SortedSetAddAsync(
                context.SemaphoreName, context.Identifier, currentScore);

            // we still have the semaphore, and have successfully updated it
            if (!wasAdded) return true;

            // else we added it because we lost the semaphore, so need to delete the just added version
            // and return false
            await Release(context);

            return false;
        }

        public async Task Release(SemaphoreContext context)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));

            var db = multiplexer.GetDatabase(database);
            await db.SortedSetRemoveAsync(context.SemaphoreName, context.Identifier);
            await db.SortedSetRemoveAsync(context.CounterSet, context.Identifier);
        }

        private double ScoreDateTime(DateTime dt) => dt.ToOADate();
    }
}
