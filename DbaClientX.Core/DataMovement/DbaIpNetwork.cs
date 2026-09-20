using System.Net;

namespace DBAClientX.DataMovement;

/// <summary>Represents an IPv4 or IPv6 network address and prefix length.</summary>
public readonly struct DbaIpNetwork : IEquatable<DbaIpNetwork>
{
    private readonly IPAddress? _address;

    /// <summary>Initializes an IP network.</summary>
    /// <param name="address">Network address.</param>
    /// <param name="prefixLength">CIDR prefix length.</param>
    public DbaIpNetwork(IPAddress address, int prefixLength)
    {
        _address = address ?? throw new ArgumentNullException(nameof(address));
        int maximum = address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 32 : 128;
        if (prefixLength < 0 || prefixLength > maximum)
            throw new ArgumentOutOfRangeException(nameof(prefixLength));
        PrefixLength = prefixLength;
    }

    /// <summary>Gets the network address.</summary>
    public IPAddress Address => _address ?? IPAddress.Any;

    /// <summary>Gets the CIDR prefix length.</summary>
    public int PrefixLength { get; }

    /// <inheritdoc />
    public bool Equals(DbaIpNetwork other)
        => PrefixLength == other.PrefixLength && Equals(Address, other.Address);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is DbaIpNetwork other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => unchecked((Address.GetHashCode() * 397) ^ PrefixLength);

    /// <inheritdoc />
    public override string ToString() => Address + "/" + PrefixLength.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Determines whether two IP networks are equal.</summary>
    public static bool operator ==(DbaIpNetwork left, DbaIpNetwork right) => left.Equals(right);

    /// <summary>Determines whether two IP networks differ.</summary>
    public static bool operator !=(DbaIpNetwork left, DbaIpNetwork right) => !left.Equals(right);
}
