using System;
using System.Collections.Generic;
using System.Reflection;
using Npgsql.TypeHandlers;
using Npgsql.TypeHandlers.CompositeHandlers;
using Npgsql.Util;
using NpgsqlTypes;

namespace Npgsql.TypeMapping
{
    abstract class TypeMapperBase : INpgsqlTypeMapper
    {
        internal readonly Dictionary<string, NpgsqlTypeMapping> Mappings;
        internal readonly ResettableDictionaryFacade<string, NpgsqlTypeMapping> MappingsFacade;

        public INpgsqlNameTranslator DefaultNameTranslator { get; }

        protected TypeMapperBase(INpgsqlNameTranslator defaultNameTranslator)
        {
            DefaultNameTranslator = defaultNameTranslator ?? throw new ArgumentNullException(nameof(defaultNameTranslator));
            Mappings = new Dictionary<string, NpgsqlTypeMapping>();
            MappingsFacade = new ResettableDictionaryFacade<string, NpgsqlTypeMapping>(Mappings);
        }

        #region Mapping management

        public virtual INpgsqlTypeMapper AddMapping(NpgsqlTypeMapping mapping)
        {
            if (Mappings.ContainsKey(mapping.PgTypeName))
                RemoveMapping(mapping.PgTypeName);
            MappingsFacade[mapping.PgTypeName] = mapping;
            return this;
        }

        public virtual bool RemoveMapping(string pgTypeName) => MappingsFacade.Remove(pgTypeName);

        IEnumerable<NpgsqlTypeMapping> INpgsqlTypeMapper.Mappings => Mappings.Values;

        public virtual void Reset() => MappingsFacade.Reset();

        #endregion Mapping management

        #region Enum mapping

        public INpgsqlTypeMapper MapEnum<TEnum>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
            where TEnum : struct, Enum
        {
            if (pgName != null && pgName.Trim() == "")
                throw new ArgumentException("pgName can't be empty", nameof(pgName));

            if (nameTranslator == null)
                nameTranslator = DefaultNameTranslator;
            if (pgName == null)
                pgName = GetPgName<TEnum>(nameTranslator);

            return AddMapping(new NpgsqlTypeMappingBuilder
            {
                PgTypeName = pgName,
                ClrTypes = new[] { typeof(TEnum) },
                TypeHandlerFactory = new EnumTypeHandlerFactory<TEnum>(nameTranslator)
            }.Build());
        }

        public bool UnmapEnum<TEnum>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
            where TEnum : struct, Enum
        {
            if (pgName != null && pgName.Trim() == "")
                throw new ArgumentException("pgName can't be empty", nameof(pgName));

            if (nameTranslator == null)
                nameTranslator = DefaultNameTranslator;
            if (pgName == null)
                pgName = GetPgName<TEnum>(nameTranslator);

            return RemoveMapping(pgName);
        }

        #endregion Enum mapping

        #region Composite mapping

        public INpgsqlTypeMapper MapComposite<T>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
            where T : new()
        {
            if (pgName != null && pgName.Trim() == "")
                throw new ArgumentException("pgName can't be empty", nameof(pgName));

            if (nameTranslator == null)
                nameTranslator = DefaultNameTranslator;
            if (pgName == null)
                pgName = GetPgName<T>(nameTranslator);

            return AddMapping(new NpgsqlTypeMappingBuilder
            {
                PgTypeName = pgName,
                ClrTypes = new[] { typeof(T) },
                TypeHandlerFactory = new CompositeTypeHandlerFactory<T>(nameTranslator)
            }.Build());
        }

        public bool UnmapComposite<T>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
            where T : new()
        {
            if (pgName != null && pgName.Trim() == "")
                throw new ArgumentException("pgName can't be empty", nameof(pgName));

            if (nameTranslator == null)
                nameTranslator = DefaultNameTranslator;
            if (pgName == null)
                pgName = GetPgName<T>(nameTranslator);

            return RemoveMapping(pgName);
        }

        #endregion Composite mapping

        #region Misc

        // TODO: why does ReSharper think `GetCustomAttribute<T>` is non-nullable?
        // ReSharper disable once ConstantConditionalAccessQualifier ConstantNullCoalescingCondition
        static string GetPgName<T>(INpgsqlNameTranslator nameTranslator)
            => typeof(T).GetCustomAttribute<PgNameAttribute>()?.PgName
               ?? nameTranslator.TranslateTypeName(typeof(T).Name);

        #endregion Misc
    }
}
