using System;
using Npgsql.TypeHandlers;
using Npgsql.TypeMapping;
using NpgsqlTypes;

// ReSharper disable once CheckNamespace
namespace Npgsql
{
    /// <summary>
    /// Extension allowing adding the Json.NET plugin to an Npgsql type mapper.
    /// </summary>
    public static class NpgsqlUtf8JsonExtensions
    {
        /// <summary>
        /// Sets up JSON.NET mappings for the PostgreSQL json and jsonb types.
        /// </summary>
        /// <param name="mapper">The type mapper to set up (global or connection-specific)</param>
        /// <param name="jsonbClrTypes">A list of CLR types to map to PostgreSQL jsonb (no need to specify NpgsqlDbType.Jsonb)</param>
        /// <param name="jsonClrTypes">A list of CLR types to map to PostgreSQL json (no need to specify NpgsqlDbType.Json)</param>
        public static INpgsqlTypeMapper UseJsonNet(
            this INpgsqlTypeMapper mapper,
            Type[] jsonbClrTypes = null,
            Type[] jsonClrTypes = null
        )
        {
            mapper.AddMapping(new NpgsqlTypeMappingBuilder
            {
                PgTypeName = "jsonb",
                NpgsqlDbType = NpgsqlDbType.Jsonb,
                ClrTypes = jsonbClrTypes,
                TypeHandlerFactory = new JsonbHandlerFactory()
            }.Build());

            /*
            mapper.AddMapping(new NpgsqlTypeMappingBuilder
            {
                PgTypeName = "json",
                NpgsqlDbType = NpgsqlDbType.Json,
                ClrTypes = jsonClrTypes,
                TypeHandlerFactory = new JsonHandlerFactory(settings)
            }.Build());
*/
            return mapper;
        }
    }
}
