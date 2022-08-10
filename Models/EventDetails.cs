using System;
using Newtonsoft.Json;

namespace SynapseRealTimeSync.Models
{
    public class EventDetails
    {
        [JsonProperty("eventType")]
        public string EventType { get; set; }
        [JsonProperty("data")]
        public object Data { get; set; }
        [JsonProperty("eventTime")]
        public DateTime EventTime { get; set; }
        [JsonProperty("subject")]
        public string Subject { get; set; }
        [JsonProperty("version")]
        public string Version { get; set; }
    }
    public class Data
    {
        [JsonProperty("fileName")]
        public string FileName { get; set; }
    }
}
