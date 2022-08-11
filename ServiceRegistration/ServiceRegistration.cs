using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SynapseRealTimeSync.ServiceRegistration
{
    public interface IServiceRegistration
	{
		void Configure(IServiceCollection services, IConfiguration configuration);
	}
}
