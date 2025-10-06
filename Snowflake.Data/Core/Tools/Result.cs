using System;

namespace Snowflake.Data.Core.Tools
{
    internal class Result<TSuccess, TError>
        where TSuccess : class
        where TError : class
    {
        public TSuccess Success { get; private set; }
        public TError Error { get; private set; }

        private Result()
        {
        }

        public static Result<TSuccess, TError> CreateResult(TSuccess success)
        {
            if (success == null)
                throw new ArgumentException("Cannot create null successful result");
            return new Result<TSuccess, TError>
            {
                Success = success,
                Error = null
            };
        }

        public static Result<TSuccess, TError> CreateError(TError error)
        {
            if (error == null)
            {
                throw new ArgumentException("Cannot create error result without error");
            }
            return new Result<TSuccess, TError>
            {
                Success = null,
                Error = error
            };
        }

        public bool IsSuccess() => Success != null;

        public bool IsError() => Error != null;
    }
}
