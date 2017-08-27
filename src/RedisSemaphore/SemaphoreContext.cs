namespace RedisSemaphore
{
    /// <summary>
    /// The context to pass back to the Release method when finished with the semaphore
    /// </summary>
    public class SemaphoreContext
    {
        internal SemaphoreContext()
        { }

        public string Identifier { get; internal set; }
        public string SemaphoreName { get; internal set; }
        public string CounterSet { get; internal set; }
    }
}
