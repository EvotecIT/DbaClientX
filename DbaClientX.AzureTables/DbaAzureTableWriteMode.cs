namespace DBAClientX.AzureTables;

/// <summary>Controls how Azure Table entities are written.</summary>
public enum DbaAzureTableWriteMode
{
    /// <summary>Insert new entities and fail when an entity already exists.</summary>
    Add,

    /// <summary>Insert entities or merge the supplied properties into existing entities.</summary>
    UpsertMerge,

    /// <summary>Insert entities or replace existing entities.</summary>
    UpsertReplace
}
