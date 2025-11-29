using System;
using System.Collections.Generic;
using System.Data.Common;

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
        /// <summary>An explicit unsupported option was detected in the connection string.</summary>
        UnsupportedOption
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

        var builder = new DbConnectionStringBuilder();
        try
        {
            builder.ConnectionString = connectionString;
        }
        catch (ArgumentException ex)
        {
            return new ConnectionValidationResult(ConnectionValidationErrorCode.MalformedConnectionString, "Connection string is malformed.", ex.Message);
        }

        // Explicitly flag "UnsupportedOption" tokens so callers can surface a deterministic message
        foreach (string key in builder.Keys)
        {
            if (string.Equals(key, "UnsupportedOption", StringComparison.OrdinalIgnoreCase))
            {
                return new ConnectionValidationResult(ConnectionValidationErrorCode.UnsupportedOption, "An unsupported connection string option was provided.", key);
            }
        }

        // Provider-specific required parameters
        switch (normalized)
        {
            case "sqlite":
                if (!HasKey(builder, "Data Source") && !HasKey(builder, "DataSource") && !HasKey(builder, "Filename"))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "SQLite connection strings must include a data source (Data Source, DataSource, or Filename).", "Data Source");
                }
                break;
            default:
                if (!HasKey(builder, "Server") && !HasKey(builder, "Data Source") && !HasKey(builder, "Host"))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "Connection string must include a server/host entry.", "Server");
                }
                if (!HasKey(builder, "Database") && !HasKey(builder, "Initial Catalog") && !HasKey(builder, "DB"))
                {
                    return new ConnectionValidationResult(ConnectionValidationErrorCode.MissingRequiredParameter, "Connection string must include a database/catalog entry.", "Database");
                }
                break;
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
            _ => result.Message
        };

    private static bool HasKey(DbConnectionStringBuilder builder, string key)
        => builder.ContainsKey(key);
}
