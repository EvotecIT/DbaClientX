using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;

namespace DBAClientX.Invoker;

/// <summary>
/// Validates provider aliases and connection strings before attempting to invoke provider-specific executors.
/// </summary>
public static class DbaConnectionFactory
{
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

    private static readonly IReadOnlyList<string[]> RequiredServerAndDatabase = new List<string[]>
    {
        new[] { "Server", "Data Source", "Host" },
        new[] { "Database", "Initial Catalog", "DB" }
    };

    private static readonly Dictionary<string, ProviderValidationProfile> ProviderProfiles = new(StringComparer.OrdinalIgnoreCase)
    {
        ["sqlserver"] = new("sqlserver", RequiredServerAndDatabase),
        ["postgresql"] = new("postgresql", RequiredServerAndDatabase, ValidatePortRange),
        ["mysql"] = new("mysql", RequiredServerAndDatabase, builder => ValidatePortRange(builder) ?? ValidateMySqlOptions(builder)),
        ["sqlite"] = new("sqlite", new List<string[]>
        {
            new[] { "Data Source", "DataSource", "Filename" }
        }, ValidateSqlitePath),
        ["oracle"] = new("oracle", RequiredServerAndDatabase)
    };

    private static readonly Dictionary<string, string> ProviderAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["sqlserver"] = "sqlserver",
        ["mssql"] = "sqlserver",
        ["postgres"] = "postgresql",
        ["postgresql"] = "postgresql",
        ["pgsql"] = "postgresql",
        ["mysql"] = "mysql",
        ["sqlite"] = "sqlite",
        ["oracle"] = "oracle"
    };

    private static readonly ThreadLocal<DbConnectionStringBuilder> BuilderCache = new(() => new DbConnectionStringBuilder());

    private static readonly Dictionary<string, string> DisallowedOptions = new(StringComparer.OrdinalIgnoreCase)
    {
        ["AllowLoadLocalInfile"] = "Loading local files is disabled for security reasons.",
        ["LoadLocalInfile"] = "Loading local files is disabled for security reasons.",
        ["Use Procedure Bodies"] = "Using procedure bodies is disallowed due to injection risk."
    };

    /// <summary>
    /// Validates a provider alias and connection string combination and returns a structured result describing any issues.
    /// </summary>
    public static ConnectionValidationResult Validate(string providerAlias, string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(providerAlias))
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingProvider, "Provider alias is required.");
        }

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingConnectionString, "Connection string is required.");
        }

        if (!ProviderAliases.TryGetValue(providerAlias, out var normalized))
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedProvider, $"Provider '{providerAlias}' is not supported.");
        }

        var builder = BuilderCache.Value!;
        builder.Clear();
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
        catch (Exception ex)
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MalformedConnectionString, "Connection string is malformed.", SanitizeExceptionMessage(ex));
        }

        var disallowedResult = ValidateDisallowedOptions(builder);
        if (disallowedResult != null)
        {
            return disallowedResult;
        }

        var profile = ProviderProfiles.GetValueOrDefault(normalized) ?? new ProviderValidationProfile(normalized, RequiredServerAndDatabase);

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
            if (!alternatives.Any(builder.ContainsKey))
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
            if (builder.ContainsKey(key) && builder[key] is string value && !string.IsNullOrWhiteSpace(value))
            {
                if (!int.TryParse(value, out var port) || port is < 1 or > 65535)
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.InvalidParameterValue, "Port must be between 1 and 65535.", key);
                }
            }
        }

        return null;
    }

    private static ConnectionValidationResult? ValidateSqlitePath(DbConnectionStringBuilder builder)
    {
        foreach (var key in new[] { "Data Source", "DataSource", "Filename" })
        {
            if (builder.ContainsKey(key) && builder[key] is string path)
            {
                if (path.Contains("..", StringComparison.Ordinal))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.InvalidParameterValue, "SQLite data source contains an unsafe relative path.", key);
                }
                break;
            }
        }

        return null;
    }

    private static ConnectionValidationResult? ValidateMySqlOptions(DbConnectionStringBuilder builder)
    {
        if (builder.TryGetValue("SslMode", out var sslMode) && sslMode is string sslValue && sslValue.Equals("None", StringComparison.OrdinalIgnoreCase))
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedOption, "MySQL connections must use SSL (SslMode cannot be None).", "SslMode");
        }

        return null;
    }

    private static ConnectionValidationResult? ValidateDisallowedOptions(DbConnectionStringBuilder builder)
    {
        foreach (string key in builder.Keys)
        {
            if (DisallowedOptions.TryGetValue(key, out var message))
            {
                return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedOption, message, key);
            }
        }

        return null;
    }

    private static string SanitizeExceptionMessage(Exception ex)
        => ex.GetType().Name;
}
