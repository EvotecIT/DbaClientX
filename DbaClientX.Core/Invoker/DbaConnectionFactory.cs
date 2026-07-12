using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;

namespace DBAClientX.Invoker;

/// <summary>
/// Validates provider aliases and connection strings before attempting to invoke provider-specific executors.
/// </summary>
public static class DbaConnectionFactory
{
    /// <summary>Describes a supported provider and the shared aliases used to resolve it.</summary>
    public sealed record ProviderDescriptor(
        string CanonicalName,
        IReadOnlyList<string> Aliases,
        string GenericExecutorTypeName);

    /// <summary>
    /// Represents the type of validation failure encountered while preparing a connection.
    /// </summary>
    public enum ConnectionValidationErrorCode
    {
        /// <summary>No validation issues were found.</summary>
        None,
        /// <summary>The provider alias was missing or whitespace.</summary>
        MissingProvider,
        /// <summary>The connection string was missing or whitespace.</summary>
        MissingConnectionString,
        /// <summary>The connection string could not be parsed by <see cref="DbConnectionStringBuilder"/>.</summary>
        MalformedConnectionString,
        /// <summary>The supplied provider alias is not supported by the invoker.</summary>
        UnsupportedProvider,
        /// <summary>A required connection string parameter was not found.</summary>
        MissingRequiredParameter,
        /// <summary>An explicit unsupported or disallowed option was detected in the connection string.</summary>
        UnsupportedOption,
        /// <summary>A connection string parameter contained an invalid or unsafe value.</summary>
        InvalidParameterValue
    }

    /// <summary>
    /// Captures the outcome of validating a provider alias and connection string pair.
    /// </summary>
    /// <param name="Code">Error code describing the validation outcome.</param>
    /// <param name="Message">Human-friendly message associated with the validation result.</param>
    /// <param name="Details">Optional details such as a missing parameter name or unsupported option.</param>
    public sealed record ConnectionValidationResult(ConnectionValidationErrorCode Code, string Message, string? Details = null)
    {
        /// <summary>Convenience property that indicates a successful validation.</summary>
        public bool IsValid => Code == ConnectionValidationErrorCode.None;
    }

    private sealed record ProviderValidationProfile(
        string NormalizedName,
        IReadOnlyList<string[]> RequiredParameters,
        Func<DbConnectionStringBuilder, ConnectionValidationResult?>? AdditionalValidation = null);

    private static readonly IReadOnlyList<string[]> RequiredServerAndDatabase = new[]
    {
        new[] { "Server", "Data Source", "Host" },
        new[] { "Database", "Initial Catalog", "DB" }
    };

    private static readonly Dictionary<string, ProviderValidationProfile> ProviderProfiles = new(StringComparer.OrdinalIgnoreCase)
    {
        ["sqlserver"] = new("sqlserver", RequiredServerAndDatabase, ValidateSqlServerOptions),
        ["postgresql"] = new("postgresql", RequiredServerAndDatabase, builder => ValidatePortRange(builder) ?? ValidatePostgreSqlOptions(builder)),
        ["mysql"] = new("mysql", RequiredServerAndDatabase, builder => ValidatePortRange(builder) ?? ValidateMySqlOptions(builder)),
        ["sqlite"] = new("sqlite", new[]
        {
            new[] { "Data Source", "DataSource", "Filename", "FullUri" }
        }, ValidateSqlitePath),
        ["oracle"] = new("oracle", new[]
        {
            new[] { "Data Source", "DataSource" }
        }, ValidateOracleAuthentication)
    };

    private static readonly IReadOnlyList<ProviderDescriptor> ProviderDescriptorList = Array.AsReadOnly(new[]
    {
        new ProviderDescriptor("sqlserver", Array.AsReadOnly(new[] { "sqlserver", "mssql" }), "DBAClientX.SqlServerGeneric.GenericExecutors"),
        new ProviderDescriptor("postgresql", Array.AsReadOnly(new[] { "postgresql", "postgres", "pgsql" }), "DBAClientX.PostgreSqlGeneric.GenericExecutors"),
        new ProviderDescriptor("mysql", Array.AsReadOnly(new[] { "mysql" }), "DBAClientX.MySqlGeneric.GenericExecutors"),
        new ProviderDescriptor("sqlite", Array.AsReadOnly(new[] { "sqlite" }), "DBAClientX.SQLiteGeneric.GenericExecutors"),
        new ProviderDescriptor("oracle", Array.AsReadOnly(new[] { "oracle" }), "DBAClientX.OracleGeneric.GenericExecutors")
    });

    private static readonly Dictionary<string, ProviderDescriptor> ProvidersByAlias = CreateProviderAliasMap();

    private static readonly Dictionary<string, string> DisallowedOptions = new(StringComparer.OrdinalIgnoreCase)
    {
        ["AllowLoadLocalInfile"] = "Loading local files is disabled for security reasons.",
        ["Allow Load Local Infile"] = "Loading local files is disabled for security reasons.",
        ["LoadLocalInfile"] = "Loading local files is disabled for security reasons.",
        ["Use Procedure Bodies"] = "Using procedure bodies is disallowed due to injection risk."
    };

    /// <summary>Gets the providers supported by the shared validation and invoker surfaces.</summary>
    public static IReadOnlyList<ProviderDescriptor> SupportedProviders => ProviderDescriptorList;

    /// <summary>Resolves a provider alias to its shared descriptor.</summary>
    public static bool TryGetProvider(
        string? providerAlias,
        out ProviderDescriptor descriptor)
    {
        descriptor = null!;
        if (string.IsNullOrWhiteSpace(providerAlias)
            || !ProvidersByAlias.TryGetValue(providerAlias!.Trim(), out var resolved))
        {
            return false;
        }

        descriptor = resolved;
        return true;
    }

    /// <summary>
    /// Validates a provider alias and connection string combination and returns a structured result describing any issues.
    /// </summary>
    public static ConnectionValidationResult Validate(string providerAlias, string? connectionString)
        => Validate(providerAlias, connectionString, allowedUnsupportedOptions: null);

    /// <summary>
    /// Validates a provider alias and connection string combination while allowing explicitly scoped unsupported options.
    /// </summary>
    public static ConnectionValidationResult Validate(
        string providerAlias,
        string? connectionString,
        IReadOnlyCollection<string>? allowedUnsupportedOptions)
    {
        if (string.IsNullOrWhiteSpace(providerAlias))
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingProvider, "Provider alias is required.");
        }

        providerAlias = providerAlias.Trim();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingConnectionString, "Connection string is required.");
        }

        if (!TryGetProvider(providerAlias, out var provider))
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedProvider, $"Provider '{providerAlias}' is not supported.");
        }

        var builder = new DbConnectionStringBuilder();
        try
        {
            builder.ConnectionString = connectionString;
        }
        catch (ArgumentException ex)
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MalformedConnectionString, "Connection string is malformed.", SanitizeExceptionMessage(ex));
        }
        catch (InvalidOperationException ex)
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MalformedConnectionString, "Connection string is malformed.", SanitizeExceptionMessage(ex));
        }
        catch (Exception ex) when (ex is FormatException or NotSupportedException)
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MalformedConnectionString, "Connection string is malformed.", SanitizeExceptionMessage(ex));
        }

        var disallowedResult = ValidateDisallowedOptions(builder, allowedUnsupportedOptions);
        if (disallowedResult != null)
        {
            return disallowedResult;
        }

        ProviderProfiles.TryGetValue(provider.CanonicalName, out var profile);
        profile ??= new ProviderValidationProfile(provider.CanonicalName, RequiredServerAndDatabase);

        var requiredParameterResult = ValidateRequiredParameters(profile, builder);
        if (requiredParameterResult != null)
        {
            return requiredParameterResult;
        }

        if (profile.AdditionalValidation != null)
        {
            var result = profile.AdditionalValidation(builder);
            if (result != null)
            {
                return result;
            }
        }

        return new ConnectionValidationResult(ConnectionValidationErrorCode.None, "Connection details validated.");
    }

    /// <summary>
    /// Converts a validation result into a user-facing message suitable for CLI/PowerShell output.
    /// </summary>
    public static string ToUserMessage(ConnectionValidationResult result)
        => result.Code switch
        {
            ConnectionValidationErrorCode.None => "Connection details validated successfully.",
            ConnectionValidationErrorCode.MissingProvider => "A provider alias is required (e.g., sqlserver, pgsql, mysql, sqlite, oracle).",
            ConnectionValidationErrorCode.MissingConnectionString => "A connection string is required.",
            ConnectionValidationErrorCode.MalformedConnectionString => result.Details is null
                ? "The connection string could not be parsed."
                : $"The connection string could not be parsed: {result.Details}",
            ConnectionValidationErrorCode.UnsupportedProvider => result.Message,
            ConnectionValidationErrorCode.MissingRequiredParameter => result.Details is null
                ? result.Message
                : $"{result.Message} Missing: {result.Details}.",
            ConnectionValidationErrorCode.UnsupportedOption => result.Details is null
                ? result.Message
                : $"{result.Message} Option: {result.Details}.",
            ConnectionValidationErrorCode.InvalidParameterValue => result.Details is null
                ? result.Message
                : $"{result.Message} Parameter: {result.Details}.",
            _ => result.Message
        };

    private static ConnectionValidationResult? ValidateRequiredParameters(ProviderValidationProfile profile, DbConnectionStringBuilder builder)
    {
        foreach (var alternatives in profile.RequiredParameters)
        {
            if (!alternatives.Any(key => TryGetNonEmptyValue(builder, key, out _)))
            {
                return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, $"{profile.NormalizedName} connection strings must include {alternatives[0]}.", alternatives[0]);
            }
        }

        return null;
    }

    private static ConnectionValidationResult? ValidatePortRange(DbConnectionStringBuilder builder)
    {
        foreach (var key in new[] { "Port", "PortNumber", "Port No" })
        {
            if (TryGetNonEmptyValue(builder, key, out var value))
            {
                if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var port) || port is < 1 or > 65535)
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.InvalidParameterValue, "Port must be between 1 and 65535.", key);
                }

            }
        }

        return null;
    }

    private static ConnectionValidationResult? ValidateOracleAuthentication(DbConnectionStringBuilder builder)
    {
        if (TryGetNonEmptyValue(builder, "External Authentication", out var externalAuthentication)
            && bool.TryParse(externalAuthentication, out var usesExternalAuthentication)
            && usesExternalAuthentication)
        {
            return null;
        }

        if (TryGetNonEmptyValue(builder, "User Id", out var userId)
            || TryGetNonEmptyValue(builder, "UserID", out userId)
            || TryGetNonEmptyValue(builder, "User ID", out userId)
            || TryGetNonEmptyValue(builder, "UID", out userId))
        {
            if (string.Equals(userId, "/", StringComparison.Ordinal))
            {
                return null;
            }

            if (TryGetNonEmptyValue(builder, "Password", out _)
                || TryGetNonEmptyValue(builder, "Pwd", out _))
            {
                return null;
            }

            return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "Oracle password authentication requires Password.", "Password");
        }

        return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "Oracle connections must include User Id or enable external authentication.", "User Id");
    }

    private static ConnectionValidationResult? ValidateSqlitePath(DbConnectionStringBuilder builder)
    {
        foreach (var key in new[] { "Data Source", "DataSource", "Filename", "FullUri" })
        {
            if (!TryGetNonEmptyValue(builder, key, out var path))
            {
                continue;
            }

            if (path.Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries)
                .Any(static segment => string.Equals(segment, "..", StringComparison.Ordinal)))
            {
                return new ConnectionValidationResult(ConnectionValidationErrorCode.InvalidParameterValue, "SQLite data source contains an unsafe relative path.", key);
            }

            break;
        }

        return null;
    }

    private static ConnectionValidationResult? ValidateSqlServerOptions(DbConnectionStringBuilder builder)
    {
        foreach (var key in new[] { "Encrypt", "Encryption" })
        {
            if (TryGetNonEmptyValue(builder, key, out var encryptValue))
            {
                if (encryptValue.Equals("False", StringComparison.OrdinalIgnoreCase)
                    || encryptValue.Equals("No", StringComparison.OrdinalIgnoreCase)
                    || encryptValue.Equals("Optional", StringComparison.OrdinalIgnoreCase))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedOption, "SQL Server connections must use encryption (Encrypt cannot be False, No, or Optional).", key);
                }
            }
        }

        return null;
    }

    private static ConnectionValidationResult? ValidateMySqlOptions(DbConnectionStringBuilder builder)
    {
        var sslModeSpecified = false;
        foreach (var key in new[] { "SslMode", "SSL Mode" })
        {
            if (builder.TryGetValue(key, out var sslMode))
            {
                sslModeSpecified = true;
                var sslValue = Convert.ToString(sslMode);
                if (string.IsNullOrWhiteSpace(sslValue))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "MySQL connections must explicitly require SSL (SslMode must be Required or Verify*).", key);
                }

                sslValue = sslValue.Trim();
                if (!IsMySqlSslModeEnforcing(sslValue))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedOption, "MySQL connections must require SSL (SslMode must be Required, VerifyCA, or VerifyFull).", key);
                }
            }
        }

        if (!sslModeSpecified)
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "MySQL connections must explicitly require SSL (SslMode must be Required or Verify*).", "SslMode");
        }

        return null;
    }

    private static ConnectionValidationResult? ValidatePostgreSqlOptions(DbConnectionStringBuilder builder)
    {
        var sslModeSpecified = false;
        foreach (var key in new[] { "SslMode", "SSL Mode" })
        {
            if (builder.TryGetValue(key, out var sslMode))
            {
                sslModeSpecified = true;
                var sslValue = Convert.ToString(sslMode);
                if (string.IsNullOrWhiteSpace(sslValue))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "PostgreSQL connections must explicitly require SSL (SslMode must be Require or Verify*).", key);
                }

                sslValue = sslValue.Trim();
                if (!IsPostgreSqlSslModeEnforcing(sslValue))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedOption, "PostgreSQL connections must require SSL (SslMode must be Require, VerifyCA, or VerifyFull).", key);
                }
            }
        }

        if (!sslModeSpecified)
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "PostgreSQL connections must explicitly require SSL (SslMode must be Require or Verify*).", "SslMode");
        }

        return null;
    }

    private static bool IsMySqlSslModeEnforcing(string? sslMode)
        => sslMode is not null
           && (sslMode.Equals("Required", StringComparison.OrdinalIgnoreCase)
               || sslMode.Equals("VerifyCA", StringComparison.OrdinalIgnoreCase)
               || sslMode.Equals("VerifyFull", StringComparison.OrdinalIgnoreCase));

    private static bool IsPostgreSqlSslModeEnforcing(string? sslMode)
        => sslMode is not null
           && (sslMode.Equals("Require", StringComparison.OrdinalIgnoreCase)
               || sslMode.Equals("VerifyCA", StringComparison.OrdinalIgnoreCase)
               || sslMode.Equals("VerifyFull", StringComparison.OrdinalIgnoreCase));

    private static ConnectionValidationResult? ValidateDisallowedOptions(
        DbConnectionStringBuilder builder,
        IReadOnlyCollection<string>? allowedUnsupportedOptions)
    {
        foreach (string key in builder.Keys)
        {
            if (allowedUnsupportedOptions?.Contains(key, StringComparer.OrdinalIgnoreCase) == true)
            {
                continue;
            }

            if (DisallowedOptions.TryGetValue(key, out var message))
            {
                return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedOption, message, key);
            }
        }

        return null;
    }

    private static string SanitizeExceptionMessage(Exception ex)
        => ex.GetType().Name;

    private static bool TryGetNonEmptyValue(DbConnectionStringBuilder builder, string key, out string value)
    {
        value = string.Empty;
        if (!builder.TryGetValue(key, out var rawValue))
        {
            return false;
        }

        value = Convert.ToString(rawValue, CultureInfo.InvariantCulture) ?? string.Empty;
        return !string.IsNullOrWhiteSpace(value);
    }

    private static Dictionary<string, ProviderDescriptor> CreateProviderAliasMap()
    {
        var aliases = new Dictionary<string, ProviderDescriptor>(StringComparer.OrdinalIgnoreCase);
        foreach (var descriptor in ProviderDescriptorList)
        {
            foreach (var alias in descriptor.Aliases)
            {
                aliases.Add(alias, descriptor);
            }
        }

        return aliases;
    }
}
