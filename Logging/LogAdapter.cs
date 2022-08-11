using Microsoft.Extensions.Logging;

namespace SynapseRealTimeSync.Logging
{
    public class LoggerAdapter<T> : IAppLogger<T>
    {
       
        private readonly ILogger<T> logger;

        public LoggerAdapter(ILoggerFactory loggerFactory)
        {
            logger = loggerFactory.CreateLogger<T>();
        }

        public void LogError(string message, params object[] args)
        {
            logger.LogError(message, args);
        }

        public void LogInformation(string message, params object[] args)
        {
            logger.LogInformation(message, args);
        }

        public void LogWarning(string message, params object[] args)
        {
            logger.LogWarning(message, args);
        }
    }
}
