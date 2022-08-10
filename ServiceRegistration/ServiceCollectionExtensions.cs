using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SynapseRealTimeSync.ServiceRegistration
{
	public static class ServiceCollectionExtensions
	{
		public static void RegisterAll<T>(this IServiceCollection services, IConfiguration configuration)
		{
			typeof(T)
				.Assembly.ExportedTypes
				.Where(x => typeof(IServiceRegistration).IsAssignableFrom(x) && !x.IsInterface && !x.IsAbstract)
				.Select(Activator.CreateInstance)
				.Cast<IServiceRegistration>()
				.ToList()
				.ForEach(x => x.Configure(services, configuration));
		}
	}
}
