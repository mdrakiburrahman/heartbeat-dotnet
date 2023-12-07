using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Producer
{
    public class InstanceStatus
    {
        [JsonProperty(PropertyName = "instanceReachable")]
        public bool InstanceReachable { get; set; }

        [JsonProperty(PropertyName = "instanceVersionProbeResponse")]
        public string InstanceVersionProbeResponse { get; set; }
    }

    public class FormattedMessage
    {
        [JsonProperty(PropertyName = "lang")]
        public string Language { get; set; }

        [JsonProperty(PropertyName = "message")]
        public string Message { get; set; }
    }

    public class SubStatus
    {
        [JsonProperty(PropertyName = "formattedMessage")]
        public FormattedMessage FormattedMessage { get; set; }
    }

    public class ExtensionStatus
    {
        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }

        [JsonProperty(PropertyName = "operation")]
        public string Operation { get; set; }

        [JsonProperty(PropertyName = "configurationAppliedTime")]
        public string ConfigurationAppliedTime { get; set; }

        [JsonProperty(PropertyName = "status")]
        public string Status { get; set; }

        [JsonProperty(PropertyName = "code")]
        public int Code { get; set; }

        [JsonProperty(PropertyName = "formattedMessage")]
        public FormattedMessage FormattedMessage { get; set; }

        [JsonProperty(PropertyName = "substatus")]
        public List<SubStatus> SubStatus { get; set; }
    }

    public class ApiResponse
    {
        [JsonProperty(PropertyName = "@odata.context")]
        public string ODataContext { get; set; }

        [JsonProperty(PropertyName = "instanceName")]
        public string InstanceName { get; set; }

        [JsonProperty(PropertyName = "instanceId")]
        public string InstanceId { get; set; }

        [JsonProperty(PropertyName = "instanceStatus")]
        public InstanceStatus InstanceStatus { get; set; }

        [JsonProperty(PropertyName = "extensionStatus")]
        public ExtensionStatus ExtensionStatus { get; set; }
    }
}
