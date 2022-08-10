using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using SynapseRealTimeSync.EventGridPublisher;
using SynapseRealTimeSync.Logging;
using SynapseRealTimeSync.Models;

namespace SynapseRealTimeSync.MongoDBChangeStream
{
    public class MongoDbChangeStreamService
    {
        protected readonly IMongoClient Client;
        protected readonly IMongoDatabase Database;
        private readonly IMongoCollection<BsonDocument> collection;
        private readonly IAppLogger<MongoDbChangeStreamService> logger;
        private readonly SemaphoreSlim semaphoreSlim = new(1, 1);
        readonly EventGridPublisherService eventGridPublisherService;
        private readonly string container;
        private readonly string dLGen2AccountName;
        private readonly string dLGen2AccountKey;
        private readonly string fileSystemName;
        private readonly string dataLakeGen2Uri;
        private string filePath;

        #region Public Methods

        public MongoDbChangeStreamService(IMongoClient client, IAppLogger<MongoDbChangeStreamService> logger, 
            EventGridPublisherService eventGridPublisherService,  IConfiguration configuration)
        {
            this.Database = client.GetDatabase(configuration["mongodb-database"]);
            this.collection = this.Database.GetCollection<BsonDocument>(configuration["mongodb-collection"]);
            this.eventGridPublisherService = eventGridPublisherService;
            this.Client = client;
            this.logger = logger;
            this.dLGen2AccountName = configuration["dataLakeGen2-accountName"];
            this.dLGen2AccountKey = configuration["dataLakeGen2-accountKey"];
            this.fileSystemName = configuration["fileSystemName"];
            this.dataLakeGen2Uri = configuration["dataLakeGen2Uri"];
            this.container = configuration["container"];
           
        }

        /// <summary>
        /// Initialize Thread
        /// </summary>
        public void Init()
        {
            // ReSharper disable once AsyncVoidLambda
            new Thread(async () => await ObserveCollections()).Start();
        }
        #endregion

        #region Private Methods

        /// <summary>
        /// Observe Collection for Update, insert or Delete activities 
        /// </summary>
        /// <returns></returns>
        private async Task ObserveCollections()
        {
            // Filter definition for document updated 
            var pipelineFilterDefinition = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                .Match(x => x.OperationType == ChangeStreamOperationType.Update
                || x.OperationType == ChangeStreamOperationType.Insert
                || x.OperationType == ChangeStreamOperationType.Delete);

            // choose stream option and set data lookup for full document 
            var changeStreamOptions = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
            };

            // Watches changes on the collection , no need to user cancellation token, mongo sdk already using it
            using var cursor = await this.collection.WatchAsync(pipelineFilterDefinition, changeStreamOptions);

            await this.semaphoreSlim.WaitAsync();

            // Run watch updated operations on returned cursor  from watch async
            await this.WatchCollectionUpdates(cursor);

            // release thread
            this.semaphoreSlim.Release();
        }

        /// <summary>
        /// Mongo DB change stream to track changes on collection
        /// </summary>
        /// <param name="cursor"></param>
        /// <returns></returns>

        private async Task WatchCollectionUpdates(IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor)
        {
            await cursor?.ForEachAsync(async change =>
            {
                // If change is null - log information as null and return message
                if (change == null)
                {
                    this.logger.LogInformation("No changes tracked  by change stream  watcher");
                    return;
                }
                try
                {
                    // Deserialize full document with Plain object 
                    var updatedDocument = BsonSerializer.Deserialize<Dictionary<string, object>>(change.FullDocument);
                    
                    // remove _id aka objectId key from Mongodb
                    updatedDocument.Remove("_id");
                    
                    // Create event data object
                    filePath = $"{container}-" + Guid.NewGuid() + ".json";
                    
                    var data = new Data()
                    {
                       FileName = filePath
                    };

                    var eventDetails = new EventDetails()
                    {
                        EventType = change.OperationType.ToString(),
                        //Data = updatedDocument.ToJson(), this is actual delta data coming from Mongodb
                        Data = data,
                        EventTime = DateTime.UtcNow,
                        Subject = "MongoDB Change Stream Connector",
                        Version = "1.0"
                    };
                    
                    // Update the blob storage and then trigger the event
                    var isBlobUpdate = await UpdateStorage(updatedDocument);

                    // In case of custom events, use this code
                    if(isBlobUpdate)
                    {
                        this.logger.LogInformation($"Changes tracked successfully by change stream : {eventDetails.Data}");
                        var isEventGridUpdated = await eventGridPublisherService.EventGridPublisher(eventDetails);
                        //var isBlobUpdate = await UpdateStorage(updatedDocument);
                        if (isEventGridUpdated)
                            this.logger.LogInformation($"Changes pushed to ADLS Gen2 and event triggered : {eventDetails.Data}");
                        else
                        {
                            this.logger.LogError($"Changes pushed but Unable to trigger event  : {eventDetails.EventType}");
                        }
                    }
                    else
                    {
                        this.logger.LogError($"Unable to push changes to blob for type : {eventDetails.EventType}");
                    }
                }
                catch (MongoException exception)
                {
                    // log mongodb exception - helpful for developers
                    this.logger.LogError("Change Stream watcher. Exception:" + exception.Message);
                }
            })!;
        }

        /// <summary>
        /// Upload delta changes to ADL gen 2
        /// </summary>
        /// <param name="updatedDocument"></param>
        /// <returns></returns>
        private async Task<bool> UpdateStorage(Dictionary<string, object> updatedDocument)
        {
            try
            {

                StorageSharedKeyCredential sharedKeyCredential =  new(dLGen2AccountName, dLGen2AccountKey);
                var dataLakeServiceClient = new DataLakeServiceClient (new Uri(dataLakeGen2Uri), sharedKeyCredential);
              
                var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);

                var directoryClient = fileSystemClient.GetDirectoryClient(container);
                
                // json file type 
                //var filePath = $"{container}-" + Guid.NewGuid() + ".json";
                DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(filePath);
                var recordContent=Encoding.UTF8.GetBytes(updatedDocument.ToJson());
                await using var ms = new MemoryStream(recordContent);
                

                //await fileClient.DeleteIfExistsAsync();
                //var file = await fileClient.UploadAsync(ms);
               
                await fileClient.AppendAsync(ms, offset: 0);
                long fileSize = recordContent.Length;
                await fileClient.FlushAsync(position: fileSize);

                await fileClient.GetAccessControlAsync();

                var accessControlList = PathAccessControlExtensions.ParseAccessControlList(
                    "user::rwx,group::rwx,other::rw-");
                await fileClient.SetAccessControlListAsync((accessControlList));
               
                var uploadedVer = fileSize > 0;
                return uploadedVer;
               
            }
            catch (Exception e)
            {
                // log exception
                this.logger.LogError("Change Stream watcher. Exception:" + e.Message);
                throw;
            }
        }

        #endregion
    }
}
