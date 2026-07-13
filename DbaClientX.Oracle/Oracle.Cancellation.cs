using System;
namespace DBAClientX;

public partial class Oracle
{
    /// <inheritdoc />
    protected override bool IsProviderCancellationException(Exception exception)
        => base.IsProviderCancellationException(exception);
}
