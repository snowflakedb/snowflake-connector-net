namespace Snowflake.Data.Core.Session
{
    internal interface ICounter
    {
        int Count();

        void Increase();

        void Decrease();

        void Reset();
    }
}
