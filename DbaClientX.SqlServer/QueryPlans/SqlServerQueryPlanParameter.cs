using System.Data;
using System.Globalization;

namespace DBAClientX.QueryPlans;

/// <summary>The SQL type of a local variable used for generic estimated planning; no value is interpolated or executed.</summary>
public sealed class SqlServerQueryPlanParameter
{
    /// <summary>Creates a type declaration for an explained parameter.</summary>
    /// <param name="type">An input scalar SQL Server type; UDT, table-valued and legacy LOB types are unsupported.</param>
    /// <param name="size">Required character/binary length; -1 means MAX for variable-length types.</param>
    /// <param name="precision">Decimal precision (1–38); defaults to 18.</param>
    /// <param name="scale">Decimal scale or time/datetime2/datetimeoffset fractional seconds; defaults to 0 or 7 respectively.</param>
    public SqlServerQueryPlanParameter(SqlDbType type, int? size = null, byte? precision = null, byte? scale = null)
    {
        Type = type;
        Size = size;
        Precision = precision;
        Scale = scale;
        Declaration = BuildDeclaration();
    }

    /// <summary>Gets the scalar SQL Server type.</summary>
    public SqlDbType Type { get; }
    /// <summary>Gets the explicit character/binary length, when applicable.</summary>
    public int? Size { get; }
    /// <summary>Gets explicit decimal precision, when supplied.</summary>
    public byte? Precision { get; }
    /// <summary>Gets explicit decimal scale or fractional-seconds precision, when supplied.</summary>
    public byte? Scale { get; }
    internal string Declaration { get; }

    private string BuildDeclaration()
    {
        if (Type is SqlDbType.Char or SqlDbType.VarChar or SqlDbType.NChar or SqlDbType.NVarChar or SqlDbType.Binary or SqlDbType.VarBinary)
        {
            bool variable = Type is SqlDbType.VarChar or SqlDbType.NVarChar or SqlDbType.VarBinary;
            int maximum = Type is SqlDbType.NChar or SqlDbType.NVarChar ? 4000 : 8000;
            if (!Size.HasValue || !(variable && Size == -1) && (Size < 1 || Size > maximum))
                throw new ArgumentOutOfRangeException(nameof(Size), "Supply a valid length, or -1 for a variable-length MAX type.");
            if (Precision.HasValue || Scale.HasValue) throw new ArgumentException("Length types do not accept precision or scale.");
            return Type.ToString() + "(" + (Size == -1 ? "max" : Size.Value.ToString(CultureInfo.InvariantCulture)) + ")";
        }
        if (Size.HasValue) throw new ArgumentException("This type does not accept a length.", nameof(Size));
        if (Type == SqlDbType.Decimal)
        {
            int precision = Precision ?? 18, scale = Scale ?? 0;
            if (precision < 1 || precision > 38 || scale > precision) throw new ArgumentOutOfRangeException(nameof(Precision), "Decimal requires precision 1–38 and scale no greater than precision.");
            return string.Format(CultureInfo.InvariantCulture, "decimal({0},{1})", precision, scale);
        }
        if (Precision.HasValue) throw new ArgumentException("Only decimal accepts precision.", nameof(Precision));
        if (Type is SqlDbType.Time or SqlDbType.DateTime2 or SqlDbType.DateTimeOffset)
        {
            if (Scale > 7) throw new ArgumentOutOfRangeException(nameof(Scale));
            return Type + "(" + (Scale ?? 7).ToString(CultureInfo.InvariantCulture) + ")";
        }
        if (Scale.HasValue) throw new ArgumentException("This type does not accept scale.", nameof(Scale));
        return Type switch
        {
            SqlDbType.BigInt or SqlDbType.Bit or SqlDbType.Date or SqlDbType.DateTime or SqlDbType.Float
                or SqlDbType.Int or SqlDbType.Money or SqlDbType.Real or SqlDbType.SmallDateTime
                or SqlDbType.SmallInt or SqlDbType.SmallMoney or SqlDbType.TinyInt or SqlDbType.UniqueIdentifier or SqlDbType.Xml
                => Type.ToString(),
            _ => throw new NotSupportedException("This scalar type is not supported for generic estimated planning.")
        };
    }
}
