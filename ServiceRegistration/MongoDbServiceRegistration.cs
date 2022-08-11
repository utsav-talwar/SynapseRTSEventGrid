using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using SynapseRealTimeSync.EventGridPublisher;
using SynapseRealTimeSync.Logging;
using SynapseRealTimeSync.MongoDBChangeStream;

namespace SynapseRealTimeSync.ServiceRegistration
{
    public class MongoDbServiceRegistration : IServiceRegistration
    {
        public void Configure(IServiceCollection services, IConfiguration configuration)
        {

            #region Register MongoDB Client driver
            services.AddSingleton<IMongoClient>(_ => new MongoClient(configuration["mongoDb-connection"]));
            #endregion

            #region Register MongoDB Change Stream Service
            services.AddSingleton<MongoDbChangeStreamService, MongoDbChangeStreamService>();
            #endregion

            #region Register Event Grid publisher Service
            services.AddSingleton<EventGridPublisherService, EventGridPublisherService>();
            #endregion

            #region Register logging Service 
            services.AddSingleton(typeof(IAppLogger<>), typeof(LoggerAdapter<>));

            #endregion
        }
    }
}
